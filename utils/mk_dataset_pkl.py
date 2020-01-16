import argparse
import json
import os
import pickle

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir")
    parser.add_argument("output_pkl_file")
    args = parser.parse_args()

    meta_map = {
        "wos_title": {},
        "wos_abstract": {},
        "wos_year": {}
    }
    for fi in os.listdir(args.input_dir):
        for line in open(os.path.join(args.input_dir, fi)):
            js = json.loads(line)

            for field in ["title", "abstract", "year"]:
                fn = "wos_"+field
                val = js[fn]
                if val not in meta_map[fn]:
                    meta_map[fn][val] = []
                meta_map[fn][val].append(js["wos_id"])

    pickle.dump(meta_map, open(args.output_pkl_file, mode="wb"))