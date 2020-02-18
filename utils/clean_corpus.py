import argparse
import copy
import json
import re
import string
import unicodedata
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from gensim.parsing.preprocessing import *
from gensim.utils import deaccent


class SimpleScrub(beam.DoFn):
    def __init__(self, fields):
        self.fields = fields

    @staticmethod
    def clean(text, delimiter = " "):
        if text is None:
            return None
        # consider stemming and removing stopwords later
        clean_string_parts = preprocess_string(text, [lambda x: unicodedata.normalize("NFKC", x), deaccent, strip_tags,
                                                 strip_punctuation, strip_numeric, strip_non_alphanum,
                                                 strip_multiple_whitespaces])
        return (delimiter.join(clean_string_parts)).strip().lower()

    def process(self, record_str):
        record = json.loads(record_str)
        for field in self.fields:
            if field not in record:
                continue
            value = record[field]
            if type(value) == list:
                record[field+"_norm"] = [self.clean(v) for v in value]
            else:
                record[field+"_norm"] = self.clean(value)
        yield record


class AggressiveScrub(beam.DoFn):
    def __init__(self, fields):
        self.fields = fields

    def strip_copyright(self, text):
        patterns = [r'(copyright)?\s+\(c\).*', r'\s+(c)\s+\d\d\d\d.*']
        clean_text = text.lower()
        for p in patterns:
            clean_text = re.sub(p, '', clean_text)
        return clean_text

    def clean_text_data(self, text, field):
        if text is None:
            return None
        # consider stemming and removing stopwords later
        cleaning_functions = [lambda x: unicodedata.normalize("NFKC", x), deaccent, strip_tags,
                              strip_punctuation, strip_numeric, strip_non_alphanum,
                              strip_multiple_whitespaces]
        if field not in ["last_names", "last_name"]:
            cleaning_functions.append(remove_stopwords)
            if field == "abstract":
                cleaning_functions.append(self.strip_copyright)
        else:
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
            else:
                cleaned = self.clean_text_data(js[field], field)
                clean_record[field+"_norm"] = " ".join(cleaned)
                clean_record[field+"_trunc_norm"] = " ".join(cleaned[:40])
                clean_record[field+"_norm_len_filt"] = " ".join([x for x in cleaned if len(x) > 6])
                clean_record[field+"_trunc_norm_len_filt"] = " ".join([x for x in cleaned[:40] if len(x) > 6])
        yield clean_record


def run_pipeline(input_dir, output_dir, fields_to_clean, do_aggressive, pipeline_args):
    if do_aggressive:
        with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
            (p | "Read from Text" >> beam.io.ReadFromText(input_dir)
                | "Aggressively Scrub Text" >> beam.ParDo(AggressiveScrub(fields_to_clean))
                | "Write to Text" >> beam.io.WriteToText(output_dir))
    else:
        with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
            (p | "Read from Text" >> beam.io.ReadFromText(input_dir)
                | "Scrub Text" >> beam.ParDo(SimpleScrub(fields_to_clean))
                | "Write to Text" >> beam.io.WriteToText(output_dir))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir")
    parser.add_argument("output_dir")
    parser.add_argument("fields_to_clean")
    parser.add_argument("--aggressive", action="store_true")
    args, pipeline_args = parser.parse_known_args()

    run_pipeline(args.input_dir, args.output_dir, args.fields_to_clean.split(","), args.aggressive, pipeline_args)



