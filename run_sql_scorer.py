import argparse
import csv
from google.cloud import bigquery


def get_num_rel(client, ak, from_key):
    query = f"SELECT count({from_key}) as num_rel from `{ak}`"
    results = client.query(query)
    return list(results)[0].get("num_rel")


def get_num_retr(client, results, from_key):
    query = f"SELECT count({from_key}) as num_retr from `{results}`"
    results = client.query(query)
    return list(results)[0].get("num_retr")


def get_num_rel_retr(client, ak, results, from_key, to_key):
    query = (f"select count(a.{from_key}) as inter from "
                f"{results} a inner join {ak} b on (a.{from_key} = b.{from_key}) and (a.{to_key} = b.{to_key})")
    results = client.query(query)
    return list(results)[0].get("inter")


def precision(n_retr, n_rel_retr):
    return n_rel_retr/n_retr


def recall(n_rel, n_rel_retr):
    return n_rel_retr/n_rel


def f1(p, r):
    return 2*p*r/(p+r)


def mk_score_row(client, info):
    n_rel = get_num_rel(client, info["ak"], info["from_key"])
    n_retr = get_num_retr(client, info["results"], info["from_key"])
    n_rel_retr = get_num_rel_retr(client, info["ak"], info["results"], info["from_key"], info["to_key"])
    p = precision(n_retr, n_rel_retr)
    r = recall(n_rel, n_rel_retr)
    return {
        "experiment_name": info["results"],
        "precision": p,
        "recall": r,
        "f1": f1(p, r)
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_csv")
    parser.add_argument("output_fi")
    args = parser.parse_args()

    client = bigquery.Client()
    out = csv.DictWriter(open(args.output_fi, mode="w"), fieldnames=["experiment_name", "precision", "recall", "f1"])
    out.writeheader()
    for line in csv.DictReader(open(args.input_csv)):
        scores = mk_score_row(client, line)
        print(scores)
        out.writerow(scores)
