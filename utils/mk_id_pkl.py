import argparse
import json
import os
import pickle

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir")
    parser.add_argument("output_pkl_file")
    args = parser.parse_args()

    id_map = {}
    for fi in os.listdir(args.input_dir):
        for line in open(os.path.join(args.input_dir, fi)):
            js = json.loads(line)
            id_map[js["wos_id"]] = {
                "wos_title": js["wos_title"],
                "wos_abstract": js["wos_abstract"],
                "wos_year": js["wos_year"]
            }

    pickle.dump(id_map, open(args.output_pkl_file, mode="wb"))