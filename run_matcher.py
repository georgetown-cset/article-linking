import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import article_linking_methods.linking_method_template


def run(input_dataset, output_dataset, pipeline_args):
    bq_input_query = f"SELECT ds_id, ds_title, ds_abstract FROM [{input_dataset}]"
    output_schema = {"fields": [
        {"name": "ds_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "wos_id", "type": "STRING", "mode": "REQUIRED"},
    ]}
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        (p | "Read from BQ" >> beam.io.Read(beam.io.BigQuerySource(query=bq_input_query))
            | "Do Match" >> beam.ParDo(article_linking_methods.linking_method_template.ExactMatchArticleLinker("gs://jtm-tmp/wos_2017.pkl"))
            | "Write to BQ" >> beam.io.WriteToBigQuery(output_dataset,
                                                       write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                       schema=output_schema))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dataset")
    parser.add_argument("output_dataset")
    args, pipeline_args = parser.parse_known_args()

    run(args.input_dataset, args.output_dataset, pipeline_args)
