import argparse
import itertools
import json
import multiprocessing
import os
import re
from simhash import Simhash, SimhashIndex


def get_features(s: str) -> list:
    '''
    The default feature extraction method, from https://github.com/leonsim/simhash
    '''
    width = 3
    s = s.lower()
    s = re.sub(r"[^\w]+", "", s)
    return [s[i:i + width] for i in range(max(len(s) - width + 1, 1))]


def write_sim_strings(data_ids_and_values: list, output_fi: str) -> None:
    '''
    Does the similarity matching and writes out the outputs. Basic method from from https://github.com/leonsim/simhash
    '''

    objs = [(article_id, Simhash(get_features(article_text))) for article_id, article_text in data_ids_and_values]
    index = SimhashIndex(objs, k=3)

    out = open(output_fi, mode="w")
    for article_id, article_text in data_ids_and_values:
        feats = Simhash(get_features(article_text))
        dup_ids = index.get_near_dups(feats)
        for dup_id in dup_ids:
            if dup_id != article_id:
                out.write(json.dumps({"id1": article_id, "id2": dup_id}) + "\n")


def get_year_partition(input_dir: str) -> dict:
    '''
    Takes an input directory of jsonl containing three fields: id, year, and normalized_text. Constructs a map
    mapping year to tuples of id, normalized_text
    :param input_dir: directory of jsonl
    :return: dict mapping year to tuples of id, normalized_text
    '''
    year_to_data_tuples = {}
    for fi in os.listdir(input_dir):
        if not fi.endswith(".jsonl"):
            continue
        if fi["year"] not in year_to_data_tuples:
            year_to_data_tuples[fi["year"]] = []
        year_to_data_tuples[fi["year"]].append((fi["id"], fi["normalized_text"]))
    return year_to_data_tuples


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir", help="directory of jsonl")
    parser.add_argument("output_dir", help=("directory where output matches should be written. "
                                            "Outputs will be in the form `year`.jsonl"))
    args = parser.parse_args()

    year_partition = get_year_partition(args.input_dir)
    with multiprocessing.Pool() as p:
        for part in year_partition:
            p.starmap(write_sim_strings,
                      [(part, os.path.join(args.output_dir, year+".jsonl")) for year, part in year_partition.items()])
