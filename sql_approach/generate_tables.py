import argparse
import itertools

from google.cloud import bigquery

from utils import create_dataset, mk_tables


def get_ascending_tuples(elts, tuple_size):
    '''
    Given a list ['arxiv', 'wos', 'ds'], returns the "ascending tuples", i.e.:
    [('arxiv', 'wos'), ('arxiv', 'ds'), ('wos', 'ds')]
    :param elts: list to create ascending tuples from
    :param tuple_size: size of tuples to create
    :return: list of ascending tuples
    '''
    # it happens that itertools.combinations has the (ascending in list order) ordering we want
    return list(itertools.combinations(elts, tuple_size))


def get_sql_sequence(sql_sequence_file, dataset_name, corpora):
    table_queries = []
    for line in open(sql_sequence_file):
        # we're using -- at the start of a line as a comment indicator
        if len(line.strip()) == 0 or line.startswith("--"):
            continue
        table_name, query_file, num_tables_str = line.strip().split("\t")
        num_tables = int(num_tables_str)
        query_template = open(query_file).read().strip()
        query = query_template.replace("{DATASET}", dataset_name)
        if num_tables > 0:
            for tpl in get_ascending_tuples(corpora, num_tables):
                query_instance = query
                table_name_instance = table_name
                for idx, elt in enumerate(tpl):
                    repl_str = "{TABLE"+str(idx+1)+"}"
                    query_instance = query_instance.replace(repl_str, elt)
                    table_name_instance = table_name_instance.replace(repl_str, elt)
                table_queries.append((table_name_instance, query_instance))
        else:
            table_queries.append((table_name, query))
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

