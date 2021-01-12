# important note: I needed to do this to get pycld2 to install on my mac:
# https://github.com/aboSamoor/pycld2/issues/13

import argparse
import ast
import chardet
import json
import logging
import pycld2 as cld2
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from typing import Iterable


class LangId(beam.DoFn):
    """
    Beam pipeline to do Language ID.
    """
    def __init__(self, fields_to_lid: list) -> None:
        self.fields_to_lid = fields_to_lid

    def add_cld2_outputs(self, record: dict) -> None:
        """
        Adds cld2 translations and confidence measures to `record`
        :param record: Record containing fields we want to run LID on. Mutated by this method.
        :return: None. Mutates `record`
        """
        for field in self.fields_to_lid:
            # ... the default success state
            record[field + "_cld2_lid_success"] = False
            if field not in record:
                continue
            try:
                try:
                    is_reliable, text_bytes_found, details = cld2.detect(record[field])
                except cld2.error as e:
                    logging.warning("utf-8 failed, attempting to use result of chardet")
                    encoding = chardet.detect(record[field].encode("utf-8"))["encoding"]
                    if encoding is None:
                        encoding = "latin-1" # last-ditch effort...
                    is_reliable, text_bytes_found, details = cld2.detect(record[field].encode("utf-8").decode(encoding))
                record[field+"_cld2_lid_success"] = True
                record[field + "_cld2_lid_is_reliable"] = is_reliable
                # details looks like: (('RUSSIAN', 'ru', 98, 404.0), ('Unknown', 'un', 0, 0.0), ('Unknown', 'un', 0, 0.0))
                # and we want the first language
                record[field + "_cld2_lid_first_result"] = details[0][0]
                record[field + "_cld2_lid_first_result_short_code"] = details[0][1]
                # convert from tuple
                #record[field + "_cld2_lid_details"] = [list(d) for d in details]
            except cld2.error as e:
                logging.warning(e)
            except UnicodeDecodeError as e:
                logging.warning(e)

    def process(self, record_str: str) -> Iterable:
        record = ast.literal_eval(record_str)
        self.add_cld2_outputs(record)
        yield json.dumps(record)

def run_pipeline(input_dir: str, output_dir: str, fields_to_lid: list, pipeline_args: list) -> None:
    """
    Run a beam pipeline that cleans all records within all files in input_dir
    :param input_dir: Directory of jsonl files to run LID on. Can be local or gcs
    :param output_dir: Directory where post-LID files should be written. Can be local or gcs
    :param fields_to_lid: Fields to run lid on within each record
    :param pipeline_args: Beam pipeline args
    :return: None
    """
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        (p | "Read from Text" >> beam.io.ReadFromText(input_dir)
            | "Run LID" >> beam.ParDo(LangId(fields_to_lid))
            | "Write to Text" >> beam.io.WriteToText(output_dir))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--fields_to_lid", required=True,
                        help="comma-separated list of fields that should have lid run on them")
    args, pipeline_args = parser.parse_known_args()

    run_pipeline(args.input_dir, args.output_dir, args.fields_to_lid.split(","), pipeline_args)