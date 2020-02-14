import argparse
import copy
import json
import os


def create_metadata_map(meta_dir):
    print("getting metadata map")
    meta_map = {}
    for fi in os.listdir(meta_dir):
        for line in open(os.path.join(meta_dir, fi)):
            js = json.loads(line)
            meta_map[js["id"]] = js
    return meta_map


def is_null(s):
    return (s is None) or (len(s.strip()) == 0)


def create_match_sets(match_dir, dataset):
    print("getting match set")
    match_set_map = {}
    for fi in os.listdir(match_dir):
        for line in open(os.path.join(match_dir,fi)):
            js = json.loads(line)
            key1 = js[dataset+"1_id"]
            key2 = js[dataset+"2_id"]
            if (key1 in match_set_map) and (key2 in match_set_map):
                set1 = match_set_map[key1]
                set2 = match_set_map[key2]
                union = set1.union(set2)
                for key in union:
                    match_set_map[key] = union
            elif key1 in match_set_map:
                match_set_map[key1].add(key2)
                match_set_map[key2] = match_set_map[key1]
            elif key2 in match_set_map:
                match_set_map[key2].add(key1)
                match_set_map[key1] = match_set_map[key2]
            else:
                pair_set = {key1, key2}
                match_set_map[key1] = pair_set
                match_set_map[key2] = pair_set
    return match_set_map


def get_best_record(record_list):
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
                row_meta_possibilities[c] = set()
            if not is_null(row[c]):
                row_meta_possibilities[c].add(row[c])

    joined_row = copy.deepcopy(min_null_row)
    for idx, col in enumerate(min_null_row):
        if is_null(min_null_row[col]):
            if len(row_meta_possibilities[col]) > 0:
                joined_row[col] = list(row_meta_possibilities[col])[0]
    return joined_row


def combine(match_set_map, meta_map, selected_metadata):
    out_combined = open(selected_metadata, mode="w")
    # now, write out the merged metadata to one record per match set
    # keep track of the sets we've seen by their object ids
    seen_set_ids = set()
    for key in match_set_map:
        match_set = match_set_map[key]
        # check if we've already processed this match set
        match_set_id = id(match_set)
        if match_set_id in seen_set_ids:
            continue
        seen_set_ids.add(match_set_id)

        meta_record_list = [meta_map[r] for r in match_set]
        best_record = get_best_record(meta_record_list)
        out_combined.write(json.dumps(best_record)+"\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("metadata_dir")
    parser.add_argument("match_dir")
    parser.add_argument("selected_metadata")
    parser.add_argument("dataset")
    args = parser.parse_args()

    meta_map = create_metadata_map(args.metadata_dir)
    match_map = create_match_sets(args.match_dir, args.dataset)
    combine(match_map, meta_map, args.selected_metadata)