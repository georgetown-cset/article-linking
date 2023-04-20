import argparse
import copy
import json
import re
import unicodedata
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from gensim.parsing.preprocessing import *
from gensim.utils import deaccent
from typing import Iterable

"""
This script normalizes string fields as needed for matching. The resulting normalized data should only be used for
matching purposes as it does not contain whitespace!
"""

class Scrub(beam.DoFn):
    """
    Beam pipeline to normalize data for linkage.
    """

    def __init__(self, fields: list) -> None:
        """
        Initialize
        :param fields: fields to clean within each record. All other fields are passed through untouched
        """
        self.fields = fields

    @staticmethod
    def strip_copyright(text: str) -> str:
        """
        Strip copyright information
        :param text: a string that may contain copyright information
        :return: the same string stripped of copyright information
        """
        patterns = [r"copyright\s+\d\d\d\d.*", r"(copyright)?\s+\(c\)\s+\d\d\d\d.*"]
        clean_text = text.lower()
        for p in patterns:
            clean_text = re.sub(p, "", clean_text)
        return clean_text.strip()

    def clean_text_data(self, value_to_clean: object, field: str) -> list:
        """
        Clean a string or list value. Behavior depends on the field the value originated from.
        :param value_to_clean: either a string or a list to clean
        :param field: the name of the field (title, abstract, etc)
        :return: an array of cleaned words in value_to_clean
        """
        if value_to_clean is None:
            return None
        cleaning_functions = [lambda x: unicodedata.normalize("NFKC", x), deaccent, strip_tags]
        if field == "abstract":
            cleaning_functions.append(self.strip_copyright)
        cleaning_functions += [strip_punctuation, strip_numeric, strip_non_alphanum]
        if field in ["last_names", "last_name"]:
            # text is a list, make it into a string
            last_names = [x.strip().split()[-1].lower() for x in value_to_clean if len(x.split()) > 0]
            value_to_clean = " ".join(sorted(last_names))
        clean_string_parts = preprocess_string(value_to_clean, cleaning_functions)
        return [x.strip().lower() for x in clean_string_parts]

    @staticmethod
    def clean_doi(doi: str) -> str:
        """
        Clean DOI. At the moment, all we do is downcase it.
        :param doi: doi string
        :return: cleaned doi string
        """
        return doi.lower().strip()

    def process(self, record_str) -> Iterable:
        """
        Load a jsonl-formatted line as json, then clean its fields
        :param record_str: jsonl-formatted string
        :return: a dict consisting of fields from record_string, where fields within self.fields have been cleaned
        """
        js = json.loads(record_str)
        clean_record = copy.deepcopy(js)
        for field in self.fields:
            if field not in js:
                continue
            if field.lower() == "doi":
                cleaned_doi = self.clean_doi(js["doi"])
                clean_record[field] = cleaned_doi if cleaned_doi else None
            elif field in ["title", "abstract", "last_name", "last_names"]:
                cleaned = self.clean_text_data(js[field], field)
                delimiter = "" if field in ["title", "abstract"] else " "
                clean_record[field+"_norm"] = delimiter.join(cleaned) if cleaned else None
            else:
                raise ValueError(field+" is not supported by clean_corpus")
        yield clean_record


def run_pipeline(input_dir: str, output_dir: str, fields_to_clean: list, pipeline_args: list) -> None:
    """
    Run a beam pipeline that cleans all records within all files in input_dir
    :param input_dir: Directory of jsonl files to clean. Can be local or gcs
    :param output_dir: Directory where cleaned jsonl files should be written. Can be local or gcs
    :param fields_to_clean: Fields to clean within each record
    :param pipeline_args: Beam pipeline args
    :return: None
    """
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        (p | "Read from Text" >> beam.io.ReadFromText(input_dir)
            | "Scrub Text" >> beam.ParDo(Scrub(fields_to_clean))
            | "Write to Text" >> beam.io.WriteToText(output_dir))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--fields_to_clean", required=True,
                        help="comma-separated list of fields that should be cleaned within each record")
    args, pipeline_args = parser.parse_known_args()

    run_pipeline(args.input_dir, args.output_dir, args.fields_to_clean.split(","), pipeline_args)



