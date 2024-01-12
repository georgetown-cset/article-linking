import argparse
import json
import os
import pickle

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source")
    parser.add_argument("input_dir")
    parser.add_argument("output_pkl_file")
    args = parser.parse_args()

    id_map = {}
    for fi in os.listdir(args.input_dir):
        for line in open(os.path.join(args.input_dir, fi)):
            js = json.loads(line)
            id_map[js[args.source + "_id"]] = {
                args.source + "_title": js[args.source + "_title"],
                args.source + "_abstract": js[args.source + "_abstract"],
                args.source
                + "_last_names": " ".join(
                    sorted([x.split()[-1] for x in js[args.source + "_last_names"]])
                ),
            }

    pickle.dump(id_map, open(args.output_pkl_file, mode="wb"))
