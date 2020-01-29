import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from article_linking_methods.full_comparison import FullComparisonLinkerTitleFilter


def run(input_dataset, output_dataset, pipeline_args):
#    bq_input_query = f"SELECT ds_id, ds_year, ds_title, ds_abstract, ds_last_names FROM [{input_dataset}]"
#    output_schema = {"fields": [
#        {"name": "ds_id", "type": "STRING", "mode": "REQUIRED"},
#        {"name": "wos_id", "type": "STRING", "mode": "REQUIRED"},
#    ]}
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        #input = p | "Read from BQ" >> beam.io.Read(beam.io.BigQuerySource(query=bq_input_query))
        input = p | "Read from GCS" >> beam.io.ReadFromText(input_dataset)
        for threshold_int in [5, 7, 9]:
            threshold = threshold_int/10
            (input | "Do Exact Match With Backoff "+str(threshold) >>
                        beam.ParDo(FullComparisonLinkerTitleFilter(#"gs://jtm-tmp/wos_5K.pkl",
                                                                    "gs://jtm-tmp/wos_5K_ids.pkl",
                                                                    threshold))

#                    | "Write to BQ "+str(threshold) >> beam.io.WriteToBigQuery(output_dataset+str(threshold_int),
#                                                       write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
#                                                       schema=output_schema))
                    | "Write to GCS "+str(threshold) >> beam.io.WriteToText(output_dataset+str(threshold_int)))



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
