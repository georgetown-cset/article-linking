import argparse
import json
import os

from tqdm import tqdm

'''
Creates match sets from pairs of linked articles, assigns each match set an id, and writes out a mapping from
each id to each article in its match set.
'''

def create_cset_article_id(idx: int):
    '''
    Create CSET article id, e.g. carticle_0000000001
    :param idx: article number
    :return: string in the form carticle_0000000001
    '''
    zero_padding = "0"*(10-len(str(idx)))
    return f"carticle_{zero_padding}{idx}"


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


def create_match_sets(match_dir: str) -> list:
    '''
    Given a directory of exported jsonl files containing article matches, generates a list of sets of matched articles,
    including "transitive matches".
    :param match_dir: directory of exported jsonl files containing article matches, with keys "`dataset`1_id" and "`dataset`2_id"
    :return: list of sets of matched articles
    '''
    print("getting adjacency lists")
    adj_list = {}
    for fi in tqdm(os.listdir(match_dir)):
        for line in open(os.path.join(match_dir, fi)):
            js = json.loads(line)
            key1 = js["id1"]
            key2 = js["id2"]
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


def create_match_keys(match_sets: list, match_file: str, prev_id_mapping_dir: str = None):
    '''
    Given a match set, creates an id for that match set, and writes out a jsonl mapping each article in the match
    set to that id
    :param match_sets: list of match sets
    :param match_file: file where id mapping should be written
    :param prev_id_mapping_dir: optional dir containing previous id mappings in jsonl form
    :return: None
    '''
    match_id = 0
    out = open(match_file, mode="w")
    prev_orig_to_merg = {}
    max_merg = "carticle_0"
    if prev_id_mapping_dir is not None:
        for fi in os.listdir(prev_id_mapping_dir):
            for line in open(os.path.join(prev_id_mapping_dir, fi)):
                js = json.loads(line.strip())
                orig_id = js["orig_id"]
                merg_id = js["merged_id"]
                assert orig_id not in prev_orig_to_merg
                prev_orig_to_merg[orig_id] = merg_id
                if merg_id > max_merg:
                    max_merg = merg_id
    match_id = int(max_merg.split("carticle_")[1])+1
    num_new, num_old = 0, 0
    for ms in tqdm(match_sets):
        cset_article_id = None
        # if we have exactly one existing id, reuse it, even if new articles are matched to it.
        # if two articles that previously had different carticle ids are now in the same match set, 
        # create a new carticle id
        existing_ids = set([prev_orig_to_merg[m] for m in ms if m in prev_orig_to_merg])
        if len(existing_ids) == 1:
            cset_article_id = existing_ids.pop()
            num_old += 1
        else:
            cset_article_id = create_cset_article_id(match_id)
            num_new += 1
            match_id += 1
        for article in ms:
            out.write(json.dumps({
                "merged_id": cset_article_id,
                "orig_id": article
            })+"\n")
    print(f"wrote {num_new} new ids and reused {num_old} ids")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--match_dir", required=True,
                        help="directory of exported jsonl from bigquery containing pairs of article matches")
    parser.add_argument("--merge_file", required=True, help="file where merged ids should be written")
    parser.add_argument("--prev_id_mapping_dir",
                        help="directory of exported jsonl from bigquery containing pairs of article matches")
    args = parser.parse_args()

    match_sets = create_match_sets(args.match_dir)
    create_match_keys(match_sets, args.merge_file, args.prev_id_mapping_dir)
