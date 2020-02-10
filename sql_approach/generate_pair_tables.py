import argparse

from google.cloud import bigquery

from utils import create_dataset, mk_tables


def get_ascending_pairs(elts):
    '''
    Given a list ['arxiv', 'wos', 'ds'], returns the "ascending pairs", i.e.:
    [('arxiv', 'wos'), ('arxiv', 'ds'), ('wos', 'ds')]
    :param elts: list to create ascending pairs from
    :return: list of ascending pairs
    '''
    pairs = []
    for i in range(len(elts)):
        for j in range(i+1, len(elts)):
            pairs.append((elts[i], elts[j]))
    return pairs


def get_sql_sequence(sql_sequence_file, dataset_name, corpora):
    table_queries = []
    for line in open(sql_sequence_file):
        if len(line.strip()) == 0:
            continue
        table_name, query_file = line.strip().split("\t")
        query_template = open(query_file).read().strip()
        query = query_template.replace("{DATASET}", dataset_name)
        for sm, lg in get_ascending_pairs(corpora):
            corpus_pair_query = query.replace("{SMALL_TABLE}", sm)
            corpus_pair_query = corpus_pair_query.replace("{LARGE_TABLE}", lg)
            pair_table_name = table_name.replace("{SMALL_TABLE}", sm).replace("{LARGE_TABLE}", lg)
            table_queries.append((pair_table_name, corpus_pair_query))
    return table_queries


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source_dataset_name")
    parser.add_argument("target_dataset_name")
    parser.add_argument("--corpora", help="comma-separated list of corpora, ordered by size",
                            default="arxiv,wos,ds,mag")
    parser.add_argument("--sql_sequence_file", default="sequences/generate_pairs.tsv")
    args = parser.parse_args()

    corpora = args.corpora.split(",")

    client = bigquery.Client()
    # TODO: make a decision about how to handle recreating vs overwriting old datasets/tables
    if args.source_dataset_name != args.target_dataset_name:
        create_dataset(args.target_dataset_name, client)

    sql_sequence = get_sql_sequence(args.sql_sequence_file, args.source_dataset_name, corpora)
    mk_tables(client, args.target_dataset_name, sql_sequence)

