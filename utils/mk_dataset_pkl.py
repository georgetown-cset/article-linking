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
         "wos_abstract": {}
    }
    for fi in os.listdir(args.input_dir):
         for line in open(os.path.join(args.input_dir, fi)):
              js = json.loads(line)

              title = js["wos_title"]
              if title not in meta_map["wos_title"]:
                   meta_map["wos_title"][title] = []
              meta_map["wos_title"][title].append(js["wos_id"])

              abs = js["wos_abstract"]
              if abs not in meta_map["wos_abstract"]:
                   meta_map["wos_abstract"][abs] = []
              meta_map["wos_abstract"][abs].append(js["wos_id"])

    pickle.dump(meta_map, open(args.output_pkl_file, mode="wb"))