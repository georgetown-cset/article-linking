import argparse
import copy
import json
import re
import unicodedata
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from gensim.parsing.preprocessing import *
from gensim.utils import deaccent


class Scrub(beam.DoFn):
    def __init__(self, fields):
        self.fields = fields

    def strip_copyright(self, text):
        patterns = [r'(copyright)?\s+\(c\).*', r'\s+(c)\s+\d\d\d\d.*']
        clean_text = text.lower()
        for p in patterns:
            clean_text = re.sub(p, '', clean_text)
        return clean_text.strip()

    def clean_text_data(self, text, field):
        if text is None:
            return None
        cleaning_functions = [lambda x: unicodedata.normalize("NFKC", x), deaccent, strip_tags]
        if field == "abstract":
            cleaning_functions.append(self.strip_copyright)
        cleaning_functions += [strip_punctuation, strip_numeric, strip_non_alphanum]
        if field in ["last_names", "last_name"]:
            # text is a list, make it into a string
            last_names = [x.strip().split()[-1] for x in text if len(x.split()) > 0]
            text = " ".join(sorted(last_names))
        clean_string_parts = preprocess_string(text, cleaning_functions)
        return [x.strip().lower() for x in clean_string_parts]

    def clean_doi(self, doi):
        return doi.lower()

    def process(self, record_str):
        js = json.loads(record_str)
        clean_record = copy.deepcopy(js)
        for field in self.fields:
            if field not in js:
                continue
            if field.lower() == "doi":
                clean_record[field] = self.clean_doi(js["doi"])
            elif field in ["title", "abstract", "last_name", "last_names"]:
                cleaned = self.clean_text_data(js[field], field)
                delimiter = "" if field in ["title", "abstract"] else " "
                clean_record[field+"_norm"] = delimiter.join(cleaned)
        yield clean_record


def run_pipeline(input_dir, output_dir, fields_to_clean, pipeline_args):
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        (p | "Read from Text" >> beam.io.ReadFromText(input_dir)
            | "Scrub Text" >> beam.ParDo(Scrub(fields_to_clean))
            | "Write to Text" >> beam.io.WriteToText(output_dir))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir")
    parser.add_argument("output_dir")
    parser.add_argument("fields_to_clean")
    args, pipeline_args = parser.parse_known_args()

    run_pipeline(args.input_dir, args.output_dir, args.fields_to_clean.split(","), pipeline_args)



