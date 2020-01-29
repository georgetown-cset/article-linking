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

    fields = [args.source+"_title", args.source+"_abstract", args.source+"_last_names"]
    meta_map = { f: {} for f in fields }
    for fi in os.listdir(args.input_dir):
        for line in open(os.path.join(args.input_dir, fi)):
            js = json.loads(line)
            for field in fields:
                val = js[field]
                if field.endswith("_names"):
                    val = " ".join(sorted([x.split()[-1] for x in js[field]]))
                if val not in meta_map[field]:
                    meta_map[field][val] = []
                meta_map[field][val].append(js[args.source+"_id"])

    pickle.dump(meta_map, open(args.output_pkl_file, mode="wb"))