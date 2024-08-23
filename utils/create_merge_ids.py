"""
Creates match sets from pairs of linked articles, assigns each match set an id, and writes out a mapping from
each id to each article in its match set.
"""

import argparse
import json
import multiprocessing
import os


def create_cset_article_id(idx: int):
    """
    Create CSET article id, e.g. carticle_0000000001
    :param idx: article number
    :return: string in the form carticle_0000000001
    """
    zero_padding = "0" * (10 - len(str(idx)))
    return f"carticle_{zero_padding}{idx}"


def get_connected_edges(adj_list: dict, key: str) -> set:
    """
    Given a dict where a key-value pair corresponds to an article match and a particular article `key`,
    returns a set of articles matched to `key`.
    :param adj_list: a dict of key-value pairs corresponding to matched articles
    :param key: an article to match in `adj_list`
    :return: a set of matched articles
    """
    conn_edges = {key}
    to_explore = adj_list[key]
    while len(to_explore) > 0:
        v = to_explore.pop()
        if v not in conn_edges:
            conn_edges.add(v)
            to_explore = to_explore.union(
                {k for k in adj_list[v] if k not in conn_edges}
            )
    return conn_edges


def get_exclude_matches(exclude_dir: str) -> dict:
    """
    Build dict mapping ids to sets of other ids they should not be matched to
    :param exclude_dir: directory of jsonl files containing article pairs that should not be matched together
    :return: dict mapping an id to a set of ids that are not valid matches
    """
    dont_match = {}
    if not exclude_dir:
        return dont_match
    for fi in os.listdir(exclude_dir):
        with open(os.path.join(exclude_dir, fi)) as f:
            for line in f:
                js = json.loads(line)
                if js["id1"] not in dont_match:
                    dont_match[js["id1"]] = set()
                if js["id2"] not in dont_match:
                    dont_match[js["id2"]] = set()
                dont_match[js["id1"]].add(js["id2"])
                dont_match[js["id2"]].add(js["id1"])
    return dont_match


def create_match_sets(
    exact_match_dir: str, simhash_match_dir: str, exclude_dir: str = None
) -> list:
    """
    Given a directory of exported jsonl files containing article matches, generates a list of sets of matched articles,
    including "transitive matches". We will use the ids present in the exact matches to filter the simhash matches,
    since it's possible for obsolete ids to live on in the simhash index
    :param exact_match_dir: directory of jsonls containing matched orig_ids from exact metadata match
    :param simhash_match_dir: directory of jsonls containing matched orig_ids from simhash
    :param exclude_dir: directory of jsonl files containing article pairs that should not be matched together
    :return: list of sets of matched articles
    """
    print("reading pairs to not match")
    dont_match = get_exclude_matches(exclude_dir)
    print("getting adjacency lists")
    adj_list = {}
    usable_ids = set()
    for match_dir, is_simhash in [(exact_match_dir, False), (simhash_match_dir, True)]:
        for fi in os.listdir(match_dir):
            with open(os.path.join(match_dir, fi)) as f:
                for line in f:
                    js = json.loads(line)
                    key1 = js["id1"]
                    key2 = js["id2"]
                    if is_simhash:
                        if (key1 not in usable_ids) or (key2 not in usable_ids):
                            continue
                    else:
                        usable_ids.add(key1)
                        usable_ids.add(key2)
                    if key1 not in adj_list:
                        adj_list[key1] = set()
                    if key2 not in dont_match.get(key1, set()):
                        adj_list[key1].add(key2)
                    # even if we're in a scenario where (according to a changed metric) A matches B but B doesn't match A,
                    # this will ensure they get added to the same match set
                    if key2 not in adj_list:
                        adj_list[key2] = set()
                    if key1 not in dont_match.get(key2, set()):
                        adj_list[key2].add(key1)
    seen_ids = set()
    match_sets = []
    for k in adj_list.keys():
        if k in seen_ids:
            continue
        # grab every connected article
        match_set = get_connected_edges(adj_list, k)
        for matched_key in match_set:
            seen_ids.add(matched_key)
        match_sets.append(match_set)
    return match_sets


def create_matches(
    match_sets: list, ids_to_drop: str, prev_id_mapping_dir: str = None
) -> iter:
    """
    Given a match set, creates an id for that match set, and writes out a jsonl mapping each article in the match
    set to that id
    :param match_sets: list of match sets
    :param match_file: file where id mapping should be written
    :param ids_to_drop: directory containing merged ids that should not be used in jsonl form
    :param prev_id_mapping_dir: optional dir containing previous id mappings in jsonl form
    :return: a generator of tuples with two elements: a list of jsons containing orig_id, merged_id matches to be
             written, and an identifier for the batch
    """
    prev_orig_to_merg = {}
    merg_to_orig = {}
    max_merg = "carticle_0"
    if prev_id_mapping_dir is not None:
        for fi in os.listdir(prev_id_mapping_dir):
            with open(os.path.join(prev_id_mapping_dir, fi)) as f:
                for line in f:
                    js = json.loads(line.strip())
                    orig_id = js["orig_id"]
                    merg_id = js["merged_id"]
                    assert orig_id not in prev_orig_to_merg
                    prev_orig_to_merg[orig_id] = merg_id
                    if merg_id not in merg_to_orig:
                        merg_to_orig[merg_id] = set()
                    merg_to_orig[merg_id].add(orig_id)
                    if merg_id > max_merg:
                        max_merg = merg_id
    ignore_ids = set()
    for fi in os.listdir(ids_to_drop):
        with open(os.path.join(ids_to_drop, fi)) as f:
            for line in f:
                js = json.loads(line.strip())
                ignore_ids.add(js["merged_id"])
    match_id = int(max_merg.split("carticle_")[1]) + 1
    batch_size = 1_000_000
    batch_count = 0
    batch = []
    for ms in match_sets:
        cset_article_id = None
        # if we have exactly one existing id, reuse it, even if new articles are matched to it.
        # if two articles that previously had different carticle ids are now in the same match set,
        # create a new carticle id
        existing_ids = set([prev_orig_to_merg[m] for m in ms if m in prev_orig_to_merg])
        if len(existing_ids) == 1 and list(existing_ids)[0] not in ignore_ids:
            cset_article_id = existing_ids.pop()
        # In some cases, merged ids can "split apart", if their constituent articles no longer
        # match. We'll detect this case by checking whether the old set of articles assigned to
        # this merged id contain any entries missing from our current set
        if (cset_article_id and (len(merg_to_orig[cset_article_id] - set(ms)) > 0)) or (
            not cset_article_id
        ):
            cset_article_id = create_cset_article_id(match_id)
            match_id += 1
        for article in ms:
            match = {"merged_id": cset_article_id, "orig_id": article}
            if len(batch) == batch_size:
                yield batch, batch_count
                batch = [match]
                batch_count += 1
            else:
                batch.append(match)
    yield batch, batch_count


def write_batch(match_batch: tuple, output_dir: str) -> None:
    """
    Write a batch of matches to disk
    :param match_batch: tuple of a list of jsons containing a merged id and orig id, and an identifier for the batch
    :param output_dir: directory where matches should be written
    :return: None
    """
    matches, batch_id = match_batch
    with open(os.path.join(output_dir, f"matches_{batch_id}.jsonl"), "w") as f:
        for match in matches:
            f.write(json.dumps(match) + "\n")


def write_matches(
    exact_match_dir,
    simhash_match_dir,
    exclude_dir,
    ids_to_drop,
    prev_id_mapping_dir,
    output_dir,
) -> None:
    """
    Generate merged id-orig id pairs and write them out as a directory of jsonls
    :param exact_match_dir: directory of jsonls containing matched orig_ids from exact metadata match
    :param simhash_match_dir: directory of jsonls containing matched orig_ids from simhash
    :param exclude_dir: directory of article pairs that should not be matched
    :param ids_to_drop: file containing ids that should not be used
    :param prev_id_mapping_dir: directory of jsonl containing previous mapping between orig ids and merged ids
    :param output_dir: directory where jsonls containing new mappings between orig ids and merged ids should be written
    :return: None
    """
    match_sets = create_match_sets(exact_match_dir, simhash_match_dir, exclude_dir)
    match_batches = create_matches(match_sets, ids_to_drop, prev_id_mapping_dir)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    with multiprocessing.Pool() as p:
        p.starmap(write_batch, ((mb, output_dir) for mb in match_batches))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--exact_match_dir",
        required=True,
        help="directory of jsonls containing matched orig_ids from exact metadata match",
    )
    parser.add_argument(
        "--simhash_match_dir",
        required=True,
        help="directory of jsonls containing matched orig_ids from simhash",
    )
    parser.add_argument(
        "--exclude_dir",
        required=True,
        help="directory of article pairs that should not be matched",
    )
    parser.add_argument(
        "--ids_to_drop",
        required=True,
        help="file containing ids that should not be used",
    )
    parser.add_argument(
        "--prev_id_mapping_dir",
        help="directory of jsonl containing previous mapping between orig ids and merged ids",
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        help="directory where jsonls containing new mappings between orig ids and "
        "merged ids should be written",
    )
    args = parser.parse_args()

    write_matches(
        args.exact_match_dir,
        args.simhash_match_dir,
        args.exclude_dir,
        args.ids_to_drop,
        args.prev_id_mapping_dir,
        args.output_dir,
    )
