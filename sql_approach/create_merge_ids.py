import argparse
import json
from .create_canonical_metadata import create_match_sets


def create_cset_article_id(idx: int):
    '''
    Create CSET article id, e.g. carticle_0000000001
    :param idx: article number
    :return: string in the form carticle_0000000001
    '''
    zero_padding = "0"*(10-len(str(idx)))
    return f"carticle_{zero_padding}{idx}"


def create_match_keys(match_sets: list, match_file: str):
    '''

    :param match_sets:
    :param match_file:
    :return:
    '''
    match_id = 0
    out = open(match_file, mode="w")
    for ms in match_sets:
        cset_article_id = create_cset_article_id(match_id)
        match_id += 1
        for article in ms:
            out.write(json.dumps({
                "merged_id": cset_article_id,
                "orig_id": article
            })+"\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("match_dir", help="directory of exported jsonl from bigquery containing pairs of article matches")
    parser.add_argument("merge_file", help="file where merged ids should be written")
    # TODO: get rid of the need for this arg
    parser.add_argument("dataset", help="dataset name to use in match_dir")
    args = parser.parse_args()

    match_sets = create_match_sets(args.match_dir, args.dataset)
    create_match_keys(match_sets, args.merge_file)