import argparse
import ast
import copy
import json
import os

from multiprocessing import Pool
from tqdm import tqdm

'''
This script contains utilities for generating sets of matched articles from pairs of matched articles, and for
merging article metadata. 
'''

def create_metadata_map_subset(meta_fi: str) -> dict:
    '''
    Loads metadata from a jsonl file into a dict. Assumes the presence of an id column, and filters "heavily normalized"
    fields ("trunc_len_filt"...)
    :param meta_fi: jsonl file containing metadata to load
    :return: dict mapping id to metadata
    '''
    meta_map = {}
    for line in open(meta_fi):
        js = ast.literal_eval(line)
        clean_js = {}
        for k in js:
            if ("_trunc" not in k) and ("_filt" not in k):
                clean_js[k] = js[k]
        meta_map[js["id"]] = clean_js
    return meta_map


def create_metadata_map(meta_dir: str) -> dict:
    '''
    Creates a metadata map from a directory of jsonl metadata files
    :param meta_dir: directory of jsonl metadata files
    :return: dict mapping id to metadata
    '''
    print("getting metadata maps")
    meta_map = {}
    with Pool() as p:
        metadata_maps = p.map(create_metadata_map_subset, [os.path.join(meta_dir, fi) for fi in os.listdir(meta_dir)])
        print("merging metadata maps")
        for mm in tqdm(metadata_maps):
            meta_map.update(mm)
    return meta_map


def is_null(s: object) -> bool:
    '''
    Our local definition of whether a metadata field is null. We do not consider a false boolean field to be a null.
    :param s: object
    :return: a boolean, True if not null
    '''
    if s is None:
        return True
    if type(s) == list:
        return len(s) == 0
    if type(s) == bool:
        return False
    if type(s) == str:
        return len(s.strip()) == 0
    raise ValueError(type(s))


def get_connected_edges(adj_list: dict, key: str) -> set:
    '''
    Given a dict where a key-value pair corresponds to an article match and a particular article `key`,
    returns a set of articles matched to `key`.
    :param adj_list: a dict of key-value pairs corresponding to matched articles
    :param key: an article to match in `adj_list`
    :return: a set of matched articles
    '''
    conn_edges = {key}
    to_explore = adj_list[key]
    while len(to_explore) > 0:
        v = to_explore.pop()
        if v not in conn_edges:
            conn_edges.add(v)
            to_explore = to_explore.union({k for k in adj_list[v] if k not in conn_edges})
    return conn_edges


def create_match_sets(match_dir: str, dataset: str) -> list:
    '''
    Given a directory of exported jsonl files containing article matches, generates a list of sets of matched articles,
    including "transitive matches".
    :param match_dir: directory of exported jsonl files containing article matches, with keys "`dataset`1_id" and "`dataset`2_id"
    :param dataset: dataset to match in key names
    :return: list of sets of matched articles
    '''
    print("getting adjacency lists")
    adj_list = {}
    for fi in tqdm(os.listdir(match_dir)):
        for line in open(os.path.join(match_dir, fi)):
            js = json.loads(line)
            key1 = js[dataset + "1_id"]
            key2 = js[dataset + "2_id"]
            if key1 not in adj_list:
                adj_list[key1] = set()
            adj_list[key1].add(key2)
            # even if we're in a scenario where (according to a changed metric) A matches B but B doesn't match A,
            # this will ensure they get added to the same match set
            if key2 not in adj_list:
                adj_list[key2] = set()
            adj_list[key2].add(key1)
    seen_ids = set()
    match_sets = []
    for k in tqdm(adj_list.keys()):
        if k in seen_ids:
            continue
        # grab every connected article
        match_set = get_connected_edges(adj_list, k)
        for matched_key in match_set:
            seen_ids.add(matched_key)
        match_sets.append(match_set)
    return match_sets


def get_best_record(record_list: list) -> dict:
    '''
    Given a list of matched articles' metadata, returns a merged metadata record, preferentially keeping metadata
    from an article with the fewest nulls
    :param record_list: a list of matched articles' metadata
    :return: a dict of merged metadata
    '''
    min_null_row = None
    min_nulls = 100000000
    row_meta_possibilities = {}
    # get a row with fewest nulls, as well as all metadata possibilities. This row will contain the id we will use
    # for this record within-dataset going forward
    for row in record_list:
        num_nulls = sum([is_null(row[c]) for c in row])
        if num_nulls < min_nulls:
            min_nulls = num_nulls
            min_null_row = row
        for c in row:
            if c not in row_meta_possibilities:
                row_meta_possibilities[c] = []
            if not is_null(row[c]):
                row_meta_possibilities[c].append(row[c])

    joined_row = copy.deepcopy(min_null_row)
    for idx, col in enumerate(min_null_row):
        if is_null(min_null_row[col]):
            if len(row_meta_possibilities[col]) > 0:
                joined_row[col] = list(row_meta_possibilities[col])[0]
    return joined_row


def combine(match_sets: list, meta_map: dict, selected_metadata: str, match_sets_out: str) -> None:
    '''
    Combine match set metadata, and write combined data and matches out
    :param match_sets: list of matched sets of articles
    :param meta_map: dict mapping ids to metadata
    :param selected_metadata: file where selected metadata should be written
    :param match_sets_out: file where match sets should be written
    :return: None
    '''
    print("merging records")
    out_combined = open(selected_metadata, mode="w")
    out_matches = open(match_sets_out, mode="w")
    # now, write out the merged metadata to one record per match set
    for match_set in match_sets:
        meta_record_list = [meta_map[r] for r in match_set]
        best_record = get_best_record(meta_record_list)
        out_combined.write(json.dumps(best_record)+"\n")
        for match in match_set:
            out_matches.write(json.dumps({"merged_id": best_record["id"], "orig_id": match})+"\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("metadata_dir")
    parser.add_argument("match_dir")
    parser.add_argument("selected_metadata")
    parser.add_argument("match_sets_out")
    parser.add_argument("dataset")
    args = parser.parse_args()

    meta_map = create_metadata_map(args.metadata_dir)
    match_sets = create_match_sets(args.match_dir, args.dataset)
    combine(match_sets, meta_map, args.selected_metadata, args.match_sets_out)
