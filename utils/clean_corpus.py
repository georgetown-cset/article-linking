import argparse
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
    def clean(text):
        if text is None:
            return None
        # consider stemming and removing stopwords later
        clean_string_parts = preprocess_string(text, [lambda x: unicodedata.normalize("NFKC", x), deaccent, strip_tags,
                                                 strip_punctuation, strip_numeric, strip_non_alphanum,
                                                 strip_multiple_whitespaces])
        return (" ".join(clean_string_parts)).strip().lower()

    def process(self, record_str):
        record = json.loads(record_str)
        for field in self.fields:
            if field not in record:
                continue
            value = record[field]
            if type(value) == list:
                record[field] = [self.clean(v) for v in value]
            else:
                record[field] = self.clean(value)
        yield record


def run_pipeline(input_table, output_table, output_schema, pipeline_args):
    #fields_to_clean = ["ds_title", "wos_title", "ds_abstract", "wos_abstract", "ds_last_names", "wos_last_names"]
    fields_to_clean = ["title", "abstract"]
    #fields_to_clean = ["OriginalTitle"]
    #bq_input_query = f"SELECT * FROM [{input_table}]"
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        #(p | "Read from BQ" >> beam.io.Read(beam.io.BigQuerySource(query=bq_input_query, flatten_results=False))
        (p | "Read from Text" >> beam.io.ReadFromText(input_table)
            | "Scrub Text" >> beam.ParDo(SimpleScrub(fields_to_clean))
            | "Write to Text" >> beam.io.WriteToText(output_table))
#            | "Write to BQ" >> beam.io.WriteToBigQuery(output_table,
#                                                       write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
#                                                       schema=output_schema))


if __name__ == "__main__":
    """
    Sample command:
    python3 clean_corpus.py wos_dim_article_linking.filtered_doi_match_with_authors wos_dim_article_linking.clean_filtered_doi_match_with_authors --project gcp-cset-projects --disk_size_gb 30 --job_name clean-doi --save_main_session --region us-east1 --temp_location gs://cset-dataflow-test/example-tmps/ --runner DataflowRunner --requirements_file dataflow-requirements.txt
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("input_table")
    parser.add_argument("output_table")
    args, pipeline_args = parser.parse_known_args()

    output_schema = json.load(open("eval_table_schema.json"))
    run_pipeline(args.input_table, args.output_table, output_schema, pipeline_args)



