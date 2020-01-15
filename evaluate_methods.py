import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def evaluate_metrics(metrics_file, results_file, pipeline_args):
    bq_query = None
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        (p | "Read from BQ" >> beam.io.Read(beam.io.BigQuerySource(query=bq_query))
            | "Set Partition Key" >> beam.Map(lambda x: partition_method.set_partition_key(x))
            | "Partition with "+partition_method_name) >> beam.Partition(partition_method.partition_key


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--linking_method_file", default="linking_methods.csv",
                        help=("txt file with one article linking method per line, corresponding to "
                              "names of classes in article_linking_methods/"))
    parser.add_argument("--article_partitioning_method", default="PartitionOnYear")
    parser.add_argument("--input_dataset", default="gs://...")
    parser.add_argument("--test_condition", default="validation", choices=["eval", "validation"],
                        help=("One of 'validation' (a development set) or 'eval' (should only be used for evaluation "
                              "of a method developed using the 'validation' set first."))
    parser.add_argument("results_file")
    args, pipeline_args = parser.parse_known_args()

    evaluate_metrics(args.metrics_file, args.results_file, pipeline_args)