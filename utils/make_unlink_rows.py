import argparse
import csv


def make_pairs(manual_to_orig: dict) -> list:
    """
    Make all pairs of ids that should be unlinked
    :param manual_to_orig: Dict mapping manually assigned ids to original ids that we believe to be the same article
    :return: A list of pairs of ids that should not be linked together
    """
    pairs = []
    for manual1 in manual_to_orig:
        for orig1 in manual_to_orig[manual1]:
            for manual2 in manual_to_orig:
                if manual1 == manual2:
                    continue
                for orig2 in manual_to_orig[manual2]:
                    pairs.append((orig1, orig2))
    return pairs


def write_unlink_rows(unlinking_file: str, output_file: str) -> None:
    """
    Write a sql file containing a query that adds new rows to the staging_literature.unlink table
    :param unlinking_file: CSV containing two columns, `manual_id` (a manually assigned id marking articles that are the same),
      and `orig_id`, the id for the article in its source corpus
    :param output_file: SQL file containing a query that adds new rows to staging_literature.unlink
    :return: None
    """
    manual_to_orig = {}
    with open(unlinking_file) as f:
        for line in csv.DictReader(f):
            if line["manual_id"] not in manual_to_orig:
                manual_to_orig[line["manual_id"]] = set()
            manual_to_orig[line["manual_id"]].add(line["orig_id"])
    pairs = make_pairs(manual_to_orig)
    with open(output_file, mode="w") as out:
        out.write("create or replace table staging_literature.unlink as\nselect id1, id2 from staging_literature.unlink\nunion all\n")
        out.write("\nunion all\n".join([f'select "{id1}" as id1, "{id2}" as id2' for id1, id2 in pairs]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("unlinking_file", help="csv with two columns: manual_id and orig_id")
    parser.add_argument("output_file", help="file where query adding new rows should be written")
    args = parser.parse_args()

    write_unlink_rows(args.unlinking_file, args.output_file)