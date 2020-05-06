import argparse
import itertools
import json
import multiprocessing
import os
import pickle
import re
from simhash import Simhash, SimhashIndex

"""
Hacky one-time script to create the initial version of the indexes
"""


def get_features(s: str) -> list:
    '''
    The default feature extraction method, from https://github.com/leonsim/simhash
    '''
    width = 3
    s = s.lower()
    s = re.sub(r"[^\w]+", "", s)
    return [s[i:i + width] for i in range(max(len(s) - width + 1, 1))]


def write_sim_strings(data_fi: str, output_fi: str) -> None:
    '''
    Does the similarity matching and writes out the outputs. Basic method from from https://github.com/leonsim/simhash
    '''
    data_ids_and_values = [line.strip().split("\t") for line in open(data_fi).readlines()]
    objs = [(article_id, Simhash(get_features(article_text))) for article_id, article_text in data_ids_and_values]
    index = SimhashIndex(objs, k=3)
    pickle.dump(index, open(output_fi, mode="wb"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir", help="directory of jsonl")
    parser.add_argument("output_dir", help="directory where output indexes should be written")
    args = parser.parse_args()

    years = [y.strip(".tsv") for y in os.listdir(args.input_dir)]
    print("running simhash indexes")
    with multiprocessing.Pool() as p:
        p.starmap(write_sim_strings,
            [(os.path.join(args.input_dir, year+".tsv"), os.path.join(args.output_dir, year+".pkl")) for year in years])
