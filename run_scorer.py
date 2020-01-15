import argparse
import csv
from google.cloud import bigquery

# for now, this relies on the happy assumption that we can actually fit the entire set of results and the ak in memory

client = bigquery.Client()


def get_result_set(table):
    query = f"SELECT wos_id, ds_id from `{table}`"
    results = set()
    for result in client.query(query):
        results.add(result["wos_id"]+"-"+result["ds_id"])
    return results


def get_scores(ak, results, system_name):
    intersection = results.intersection(ak)
    p = len(intersection)/len(results)
    r = len(intersection)/len(ak)
    f1 = 2*p*r/(p+r)
    return {
        "system_name": system_name,
        "precision": p,
        "recall": r,
        "f1": f1
    }


def run_evaluation(ak_bq_set, test_datasets, report_file="report.csv"):
    out = csv.DictWriter(open(report_file, mode="w"), fieldnames=["system_name", "precision", "recall", "f1"])
    out.writeheader()
    print("getting answer key")
    ak_set = get_result_set(ak_bq_set)
    for dataset in test_datasets:
        print("getting results for "+dataset)
        result_set = get_result_set(dataset)
        print("getting scores for "+dataset)
        scores = get_scores(ak_set, result_set, dataset)
        out.writerow(scores)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("answer_key")
    parser.add_argument("test_datasets", help="comma-separated list of BQ tables to evaluate")
    args = parser.parse_args()

    run_evaluation(args.answer_key, args.test_datasets.strip().split(","))