import argparse
import apache_beam as beam

def evaluate_metrics(metrics_file, results_file):
    for metric in metrics_file:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics_file", default="metrics.txt")
    parser.add_argument("results_file")
    args, pipeline_args = parser.parse_known_args()

    evaluate_metrics(args.metrics_file, args.results_file)