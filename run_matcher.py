import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from article_linking_methods.linking_method_template import ExactMatchArticleLinker
from article_linking_methods.full_comparison import FullComparisonLinkerTitleFilter


def run(input_dataset, output_dataset, pipeline_args):
    bq_input_query = f"SELECT ds_id, ds_title, ds_abstract FROM [{input_dataset}]"
    output_schema = {"fields": [
        {"name": "ds_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "wos_id", "type": "STRING", "mode": "REQUIRED"},
    ]}
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        (p | "Read from BQ" >> beam.io.Read(beam.io.BigQuerySource(query=bq_input_query))
            #| "Do Exact Match" >> beam.ParDo(ExactMatchArticleLinker("gs://jtm-tmp/wos_2017.pkl"))
            | "Do Exact Match With Backoff" >> beam.ParDo(FullComparisonLinkerTitleFilter("gs://jtm-tmp/wos_2017.pkl",
                                                                                          "gs://jtm-tmp/wos_2017_ids.pkl"))
            | "Write to BQ" >> beam.io.WriteToBigQuery(output_dataset,
                                                       write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                       schema=output_schema))


if __name__ == "__main__":
    """
    Sample:
    python3 run_matcher.py wos_dim_article_linking.clean_2017 wos_dim_article_linking.test --project gcp-cset-projects --disk_size_gb 35 --job_name test-link  --save_main_session --region us-east1 --temp_location gs://cset-dataflow-test/example-tmps/ --runner DataflowRunner --setup_file ./setup.py
    TODO: read multiple linkers from the command line and automatically run eval script
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dataset")
    parser.add_argument("output_dataset")
    args, pipeline_args = parser.parse_known_args()

    run(args.input_dataset, args.output_dataset, pipeline_args)
