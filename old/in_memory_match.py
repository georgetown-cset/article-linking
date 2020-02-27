import argparse
import json
import os
import pickle
import unicodedata
from gensim.parsing.preprocessing import *
from gensim.utils import deaccent
from multiprocessing import Pool

word_length_threshold = 4


def clean_data(text, field):
    if text is None:
        return None
    # consider stemming and removing stopwords later
    cleaning_functions = [lambda x: unicodedata.normalize("NFKC", x), deaccent, strip_tags,
                                                  strip_punctuation, strip_numeric, strip_non_alphanum,
                                                  strip_multiple_whitespaces]
    if field not in ["last_names", "last_name"]:
        cleaning_functions.append(remove_stopwords)
    else:
        # text is a list, make it into a string
        last_names = [x.strip().split()[-1] for x in text if len(x.split()) > 0]
        text = " ".join(last_names)
    clean_string_parts = preprocess_string(text, cleaning_functions)
    return [x.strip().lower() for x in clean_string_parts]

def clean_and_split_data(dataset_dir, working_dir, name):
    # clean the data and split by year
    output_files = {}
    str_to_int_id_map = {}
    int_to_str_id_map = {}
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
                        int_to_str_id_map[id_counter] = str_id
                        id_counter += 1
                    clean_js[field] = str_to_int_id_map[str_id]
                elif field == "year":
                    if (js[field] is None) or (len(js[field]) == 0):
                        clean_js[field] = "none"
                    else:
                        clean_js[field] = str(js[field])
                else:
                    cleaned = clean_data(js[field], field)
                    if field == "last_name":
                        clean_js["last_names"] = cleaned
                    else:
                        clean_js[field] = cleaned
            year = clean_js["year"]
            if not year in output_files:
                year_dir = os.path.join(working_dir, year)
                if not os.path.exists(year_dir):
                    os.mkdir(year_dir)
                output_files[clean_js["year"]] = open(os.path.join(year_dir, name+"_records.jsonl"), mode="w")
            output_files[year].write(json.dumps(clean_js)+"\n")
    pickle.dump(int_to_str_id_map, open(os.path.join(working_dir, name+"_ids.pkl"), mode="wb"))


def create_index(year_path):
    print("indexing "+year_path)
    # this is potentially a massive map
    index = {}
    doc_to_unique_words = {}
    for line in open(os.path.join(year_path, "large_records.jsonl")):
        js = json.loads(line)
        uniq_words = set()
        for text_field in ["title", "abstract", "last_name"]:
            if (text_field in js) and js[text_field] is not None:
                for word in js[text_field]:
                    if len(word) > word_length_threshold:
                        uniq_words.add(word)
                        if word not in index:
                            index[word] = set()
                        index[word].add(js["id"])
        doc_to_unique_words[js["id"]] = len(uniq_words)
    pickle.dump(index, open(os.path.join(year_path, "large_index.pkl"), mode="wb"))
    pickle.dump(doc_to_unique_words, open(os.path.join(year_path, "large_counts.pkl"), mode="wb"))


def match_records(year_path):
    print("matching "+year_path)
    index = pickle.load(open(os.path.join(year_path, "large_index.pkl"), mode="rb"))
    counts = pickle.load(open(os.path.join(year_path, "large_counts.pkl"), mode="rb"))
    matches = open(os.path.join(year_path, "matches.jsonl"), mode="w")
    sm_id_map = pickle.load(open("working_dir/small_ids.pkl", mode="rb"))
    lg_id_map = pickle.load(open("working_dir/large_ids.pkl", mode="rb"))
    for line in open(os.path.join(year_path, "small_records.jsonl")):
        js = json.loads(line)
        text_words = set([x for x in js["title"]+js["abstract"]+js["last_names"] if len(x) > word_length_threshold])
        doc_to_sim = {}
        for word in text_words:
            if word not in index:
                continue
            for doc in index[word]:
                if doc not in doc_to_sim:
                    doc_to_sim[doc] = 0
                doc_to_sim[doc] += 1
        max_val, max_id = -1, None
        for k in doc_to_sim:
            if doc_to_sim[k] > max_val:
                max_val = doc_to_sim[k]
                max_id = k
        shortest = counts[max_id] if counts[max_id] < len(text_words) else len(text_words)
        if max_val/shortest >= 0.6:
            matches.write(json.dumps({"wos_id": sm_id_map[js["id"]],
                                      "ds_id": lg_id_map[max_id], "score": max_val/shortest})+"\n")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("small_dataset_dir")
    parser.add_argument("large_dataset_dir")
    parser.add_argument("working_dir")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

#    if not args.resume:
#        with Pool() as pool:
#            pool.starmap(clean_and_split_data, [[args.small_dataset_dir, args.working_dir, "small"], [args.large_dataset_dir, args.working_dir, "large"]])

    with Pool() as pool:
        years = []
        for year in os.listdir(args.working_dir):
            if year.endswith(".pkl") or year.startswith("."):
                continue
            years.append(os.path.join(args.working_dir, year))
        pool.map(create_index, years)
        pool.map(match_records, years)
