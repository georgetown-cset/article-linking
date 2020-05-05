import argparse
import itertools
import json
import multiprocessing
import os
import pickle
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


def write_sim_strings(data_fi: str, output_fi: str, input_index: str = None, output_index: str = None) -> None:
    '''
    Does the similarity matching and writes out the outputs. Basic method from from https://github.com/leonsim/simhash
    '''
    data_ids_and_values = [line.strip().split("\t") for line in open(data_fi).readlines()]
    objs = [(article_id, Simhash(get_features(article_text))) for article_id, article_text in data_ids_and_values]
    index = None
    if input_index is None:
        index = SimhashIndex(objs, k=3)
    else:
        index = pickle.load(open(input_index, mode="rb"))
        for obj_id, obj in objs:
            index.add(obj_id, obj)
        open(output_index, mode="wb").write(pickle.dumps(index))

    out = open(output_fi, mode="w")
    for article_id, article_text in data_ids_and_values:
        feats = Simhash(get_features(article_text))
        dup_ids = index.get_near_dups(feats)
        for dup_id in dup_ids:
            if dup_id != article_id:
                out.write(json.dumps({"id1": article_id, "id2": dup_id}) + "\n")


def get_year_partition(input_dir: str, output_dir: str) -> list:
    '''
    Takes an input directory of jsonl containing three fields: id, year, and normalized_text. Constructs a map
    mapping year to tuples of id, normalized_text, and writes each year's data as a tsv

    Initially I tried passing the arrays of id, normalized text for each year around in memory. However,
    the multiprocessing library pickles its inputs and some years' data exceeded the maximum pickle size.
    For the same reason, we write to tsv instead of pickling here.

    :param input_dir: directory of jsonl
    :param output_dir: dir where each year's worth of pairs should be written as pkl
    :return: list of years
    '''
    print("getting year partition")
    year_to_outfi = {}
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    for fi in os.listdir(input_dir):
        for line in open(os.path.join(input_dir, fi)):
            js = json.loads(line)
            year = js["year"]
            if year not in year_to_outfi:
                year_to_outfi[year] = open(os.path.join(output_dir, year+".tsv"), mode="w")
            year_to_outfi[year].write(f"{js['id']}\t{js['normalized_text']}\n")
    return list(year_to_outfi.keys())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir", help="directory of jsonl")
    parser.add_argument("--tmp_dir", default="simhash-tmp")
    parser.add_argument("--simhash_indexes", help="current simhash indexes")
    parser.add_argument("--new_simhash_indexes", help="location where updated indexes should be written")
    parser.add_argument("output_dir", help=("directory where output matches should be written. "
                                            "Outputs will be in the form `year`.jsonl"))
    args = parser.parse_args()

    years = get_year_partition(args.input_dir, args.tmp_dir)
    print("running simhash")
    with multiprocessing.Pool() as p:
        p.starmap(write_sim_strings,
            [(os.path.join(args.tmp_dir, year+".tsv"), os.path.join(args.output_dir, year+".jsonl"),
              None if args.simhash_indexes is None else os.path.join(args.simhash_indexes, f"{year}.pkl"),
              None if args.new_simhash_indexes is None else os.path.join(args.new_simhash_indexes, f"{year}.pkl"))
        for year in years])
