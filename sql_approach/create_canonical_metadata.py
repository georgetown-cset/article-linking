import argparse
import json
import os


def create_metadata_map(meta_dir):
    meta_map = {}
    for fi in os.listdir(meta_dir):
        for line in open(os.path.join(meta_dir, fi)):
            js = json.loads(line)
            meta_map[js["id"]] = js
    return meta_map


def is_null(s):
    return (s is None) or (len(s.strip()) == 0)


def create_match_map(match_dir, dataset):
    match_map = {}
    for fi in os.listdir(match_dir):
        for line in open(os.path.join(match_dir,fi)):
            js = json.loads(line)
            match_map[js[dataset+"1_id"]] = js[dataset+"2_id"]
    return match_map


def get_combined_map(match_map):
    pointer_map = {}
    for k, v in match_map.items():
        if (k in pointer_map) and (v in pointer_map):
            shift_key = pointer_map[v]
            for elt in pointer_map:
                if pointer_map[elt] == shift_key:
                    pointer_map[elt] = k
        elif k in pointer_map:
            pointer_map[v] = k
        elif v in pointer_map:
            pointer_map[k] = v
        else:
            pointer_map[k] = k
            pointer_map[v] = k

    combined_map = {}
    for k, v in pointer_map.items():
        if k not in combined_map:
            combined_map[k] = set()
        combined_map[k].add(v)
    return combined_map


def get_best_record(record_list):
    min_null_row = None
    min_nulls = 100000000
    row_meta_possibilities = {}
    # get a row with fewest nulls, as well as all metadata possibilities. This row will contain the id we will use
    # for this record within-dataset going forward
    for row in record_list:
        num_nulls = sum([c for c in row if is_null(c)])
        if num_nulls < min_nulls:
            min_nulls = num_nulls
            min_null_row = row
        for c in row:
            if c not in row_meta_possibilities:
                row_meta_possibilities[c] = set()
            row_meta_possibilities[c].add(row[c])

    joined_row = min_null_row
    for idx, col in enumerate(min_null_row):
        if is_null(col):
            if len(row_meta_possibilities[col]) > 0:
                joined_row[idx] = row_meta_possibilities[col][0]
                print(min_null_row)
                print(joined_row)
    return joined_row


def combine(match_map, meta_map, selected_metadata):
    out_combined = open(selected_metadata, mode="w")
    combined_map = get_combined_map(match_map)
    # now, write out all the metadata to one record per combined id
    for record_id in combined_map:
        meta_record_list = [meta_map[r] for r in combined_map[record_id]]
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
    match_map = create_match_map(args.match_dir, args.dataset)
    combine(match_map, meta_map, args.selected_metadata)