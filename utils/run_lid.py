# important note: I needed to do this to get pycld2 to install on my mac:
# https://github.com/aboSamoor/pycld2/issues/13

import argparse
import ast
import json
import logging
import unicodedata
from typing import Iterable

import apache_beam as beam
import pycld2 as cld2
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions


def sanitize_for_cld2(text: str) -> str:
    """
    Return a version of `text` that cld2 will accept.

    pycld2 only takes valid UTF-8. It raises cld2.error on lone surrogates
    (which can survive inside a Python str) and on some control / non-character
    code points. The input is already a str, so re-encoding it through other
    codecs can't fix that -- we just need to clean the str itself.
    """
    # Lone surrogates cannot be encoded as UTF-8; replace them and rebuild the str.
    text = text.encode("utf-8", "replace").decode("utf-8")
    # Fold compatibility characters (ligatures such as 'ﬃ' U+FB03, fullwidth
    # forms, etc.) into their plain equivalents. cld2's n-gram tables don't
    # score these code points, so this also improves detection on PDF-derived text.
    text = unicodedata.normalize("NFKC", text)
    # Drop control, unassigned and non-character code points; keep normal whitespace.
    return "".join(ch for ch in text if ch.isprintable() or ch in " \n\t\r")


class LangId(beam.DoFn):
    """
    Beam DoFn to do Language ID.
    """

    def __init__(self, fields_to_lid: list) -> None:
        self.fields_to_lid = fields_to_lid
        self.lid_failures = Metrics.counter(self.__class__, "cld2_lid_failures")
        self.parse_failures = Metrics.counter(self.__class__, "record_parse_failures")

    def add_cld2_outputs(self, record: dict) -> None:
        """
        Adds cld2 language predictions and confidence measures to `record`.
        :param record: Record containing fields we want to run LID on. Mutated by this method.
        :return: None. Mutates `record`
        """
        for field in self.fields_to_lid:
            record[field + "_cld2_lid_success"] = False
            value = record.get(field)
            if not isinstance(value, str) or not value:
                continue

            try:
                is_reliable, _text_bytes_found, details = cld2.detect(
                    sanitize_for_cld2(value)
                )
            except Exception as e:  # noqa: BLE001 - never let one bad record kill the bundle
                self.lid_failures.inc()
                logging.warning("cld2 failed on field %r: %s", field, e)
                continue

            record[field + "_cld2_lid_success"] = True
            record[field + "_cld2_lid_is_reliable"] = is_reliable
            # details looks like: (('RUSSIAN', 'ru', 98, 404.0), ('Unknown', 'un', 0, 0.0), ...)
            # and we want the first language
            record[field + "_cld2_lid_first_result"] = details[0][0]
            record[field + "_cld2_lid_first_result_short_code"] = details[0][1]

    def process(self, record_str: str) -> Iterable:
        try:
            record = ast.literal_eval(record_str)
        except (ValueError, SyntaxError) as e:
            self.parse_failures.inc()
            logging.warning("could not parse record: %s", e)
            return
        self.add_cld2_outputs(record)
        yield json.dumps(record)


def run_pipeline(
    input_dir: str, output_dir: str, fields_to_lid: list, pipeline_args: list
) -> None:
    """
    Run a beam pipeline that runs LID on all records within all files in input_dir
    :param input_dir: Directory of jsonl files to run LID on. Can be local or gcs
    :param output_dir: Directory where post-LID files should be written. Can be local or gcs
    :param fields_to_lid: Fields to run lid on within each record
    :param pipeline_args: Beam pipeline args
    :return: None
    """
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        (
            p
            | "Read from Text" >> beam.io.ReadFromText(input_dir)
            | "Run LID" >> beam.ParDo(LangId(fields_to_lid))
            | "Write to Text" >> beam.io.WriteToText(output_dir)
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument(
        "--fields_to_lid",
        required=True,
        help="comma-separated list of fields that should have lid run on them",
    )
    args, pipeline_args = parser.parse_known_args()

    run_pipeline(
        args.input_dir, args.output_dir, args.fields_to_lid.split(","), pipeline_args
    )
