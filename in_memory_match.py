import argparse
import json
import os
import pickle
import unicodedata
from gensim.parsing.preprocessing import *
from gensim.utils import deaccent


def clean_data(text, field):
    if text is None:
        return None
    # consider stemming and removing stopwords later
    cleaning_functions = [lambda x: unicodedata.normalize("NFKC", x), deaccent, strip_tags,
                                                  strip_punctuation, strip_numeric, strip_non_alphanum,
                                                  strip_multiple_whitespaces]
    if field != "last_names":
        cleaning_functions.append(remove_stopwords)
    else:
        # text is a list, make it into a string
        text = " ".join(text)
    clean_string_parts = preprocess_string(text, cleaning_functions)
    return [x.strip().lower() for x in clean_string_parts]

def clean_and_split_data(dataset_dir, working_dir, name):
    # clean the data and split by year
    output_files = {}
    str_to_int_id_map = {}
    id_counter = 0
    for fi in os.listdir(dataset_dir):
        for line in open(os.path.join(dataset_dir, fi)):
            js = json.loads(line)
            clean_js = {}
            if "year" not in js:
                clean_js["year"] = "none"
            for field in js:
                if field == "id":
                    str_id = js[field]
                    if str_id not in str_to_int_id_map:
                        str_to_int_id_map[str_id] = id_counter
                        id_counter += 1
                    clean_js[field] = str_to_int_id_map[str_id]
                elif field == "year":
                    if (js[field] is None) or (len(js[field]) == 0):
                        clean_js[field] = "none"
                    else:
                        clean_js[field] = str(js[field])
                else:
                    clean_js[field] = clean_data(js[field], field)
            year = clean_js["year"]
            if not year in output_files:
                year_dir = os.path.join(working_dir, year)
                if not os.path.exists(year_dir):
                    os.mkdir(year_dir)
                output_files[clean_js["year"]] = open(os.path.join(year_dir, name+"_records.jsonl"), mode="w")
            output_files[year].write(json.dumps(clean_js)+"\n")
    pickle.dump(str_to_int_id_map, open(os.path.join(working_dir, name+"_ids.pkl"), mode="wb"))


def create_index(dataset, output_pkl_name):
    # this is potentially a massive map
    index = {}
    for line in open(dataset):
        js = json.loads(line)
        for text_field in ["title", "abstract", "last_name"]:
            if (text_field in js) and js[text_field] is not None:
                for word in js[text_field]:
                    if word not in index:
                        index[word] = set()
                    index[word].add(js["id"])
    pickle.dump(index, open(output_pkl_name, mode="wb"))


#def match_records(sm_records, lg_pkl, sm_label, lg_label, out_file):
#    index = pickle.load(open(lg_pkl))
#    for line in open(sm_records):
#        js = json.loads(line)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("small_dataset_dir")
    parser.add_argument("large_dataset_dir")
    parser.add_argument("working_dir")
    args = parser.parse_args()

    clean_and_split_data(args.small_dataset_dir, args.working_dir, "small")
    clean_and_split_data(args.large_dataset_dir, args.working_dir, "large")

    for year in os.listdir(args.working_dir):
        if year.endswith(".pkl"):
            continue
        print("running "+year)
        create_index(os.path.join(args.working_dir, year, "large_records.jsonl"),
                     os.path.join(args.working_dir, year, "large_index.pkl"))
        #match_records(os.path.join(args.working_dir, year))