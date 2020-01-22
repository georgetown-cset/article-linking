import argparse
import csv
from google.cloud import bigquery


class ArticleLinkageEvaluator:
    key_joiner = "---"
    error_types = ["false_alarm", "miss"]
    from_key = "wos_id"
    to_key = "ds_id"

    def __init__(self, full_table, ak_table, experiment_output_tables, report_file, error_file_template):
        self.client = bigquery.Client()
        self.full_table = full_table
        self.ak_table = ak_table
        self.experiment_output_tables = experiment_output_tables
        self.report_file = report_file
        self.error_file_template = error_file_template

    def unmake_key(self, key):
        fk, tk = key.split(self.key_joiner)
        return {
            self.from_key: fk,
            self.to_key: tk
        }

    def mk_key(self, row):
        return row[self.from_key]+self.key_joiner+row[self.to_key]

    def get_result_set(self, table):
        query = f"SELECT {self.from_key}, {self.to_key} from `{table}`"
        results = set()
        for result in self.client.query(query):
            results.add(self.mk_key(result))
        return results

    def write_errors(self, errors, error_file):
        out = None
        for error_type in self.error_types:
            formatted_errors = ",".join([f"'{e}'" for e in errors[error_type]])
            query = f"SELECT * from `{self.full_table}` where {self.from_key} in ({formatted_errors})"
            for result in self.client.query(query):
                result["error_type"] = error_type
                # I'm sure there's a prettier way to do this, but I'm just trying to get the column names
                if out is None:
                    out = csv.DictWriter(open(error_file, mode="w"), fieldnames=list(result.keys()))
                    out.writeheader()
                out.writerow(result)

    def get_scores(self, ak, results, system_name):
        # for now, this relies on the happy assumption that we can actually fit the entire set of results
        # and the ak in memory
        intersection = results.intersection(ak)
        p = len(intersection)/len(results)
        r = len(intersection)/len(ak)
        f1 = 2*p*r/(p+r)
        # TODO: consider refactoring - this is getting too complicated
        # also TODO: the unmake_key thing on just from_key doesn't work as one from_key may get mapped to multiple FAs
        return {
            "score_info": {
                "system_name": system_name,
                "precision": p,
                "recall": r,
                "f1": f1
            },
            "errors": {
                "false_alarm": [self.unmake_key(r)[self.from_key] for r in results - ak],
                "miss": [self.unmake_key(r)[self.from_key] for r in ak - intersection]
            }
        }

    def run_evaluation(self):
        out = csv.DictWriter(open(self.report_file, mode="w"), fieldnames=["system_name", "precision", "recall", "f1"])
        out.writeheader()
        print("getting answer key")
        ak_set = self.get_result_set(self.ak_table)
        for dataset in self.experiment_output_tables:
            print("getting results for "+dataset)
            result_set = self.get_result_set(dataset)
            print("getting scores for "+dataset)
            scores = self.get_scores(ak_set, result_set, dataset)
            out.writerow(scores["score_info"])
            self.write_errors(scores["errors"], self.error_file_template.format(dataset))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("full_table")
    parser.add_argument("answer_key")
    parser.add_argument("test_dataset")
    parser.add_argument("--report_file", default="report.csv")
    parser.add_argument("--error_file_template", default="errors{}.csv")
    args = parser.parse_args()

    evaluator = ArticleLinkageEvaluator(args.full_table, args.answer_key, args.test_dataset.split(","),
                                        args.report_file, args.error_file_template)
    evaluator.run_evaluation()
