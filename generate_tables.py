import argparse
import itertools
import re

from google.cloud import bigquery

'''
This script is used to programmatically populate "sequence files" as described in `get_sql_sequence`. 
We populate the table name and the sql script with table references.
'''


def create_dataset(dataset_name: str, client) -> None:
    '''
    Creates a new BigQuery dataset. Fails if dataset exists.
    :param dataset_name: name of the dataset
    :param client: BigQuery client
    :return: null
    '''
    dataset = bigquery.Dataset("gcp-cset-projects."+dataset_name)
    dataset.location = "US"
    client.create_dataset(dataset)


def mk_tables(client, dataset_name: str, table_queries: list) -> None:
    '''
    Runs a sequence of sql queries, generating tables from each query result. Fails if the
    table already exists.
    :param client: BigQuery client
    :param dataset_name: name of the BigQuery dataset that should contain the new table
    :param table_queries: list of tuples. The first element of each tuple is the name of an output table; the second
                            element is a reference to the query that should be run to generate that table
    :return: null
    '''
    for table_name, query in table_queries:
        print(f"Running '{query}' for output to {dataset_name}.{table_name}\n")
        job_config = bigquery.QueryJobConfig(destination="gcp-cset-projects."+dataset_name+"."+table_name)
        query_job = client.query(query, job_config=job_config)
        query_job.result()


def get_ascending_tuples(elts: list, tuple_size: int) -> list:
    '''
    Given a list ['arxiv', 'wos', 'ds'], returns the "ascending tuples", i.e.:
    [('arxiv', 'wos'), ('arxiv', 'ds'), ('wos', 'ds')]
    :param elts: list to create ascending tuples from
    :param tuple_size: number of tables that should be in each tuple
    :return: list of ascending tuples
    '''
    return list(itertools.combinations(elts, tuple_size))


def get_sql_sequence(sql_sequence_file: str, dataset_name: str, corpora: list, self_match: bool) -> list:
    '''
    Parses a sql sequence file. Sequence files are tab-separated files containing three columns:
        1.) A table name
        2.) A reference to a sql script that should be run to generate that table
        3.) The number of tables that should be passed into the script (0 if no dynamic table references, 1 for only one
    table reference, 2 for multiple table references, etc.)
    :param sql_sequence_file: filename of a headerless tsv in the format described above
    :param dataset_name: name of the dataset all tables should be written to
    :param corpora: corpus table references
    :param self_match: true if all table references should be replaced with TABLE1, false otherwise
    :return: a list of tuples, where the first element of each tuple is a table name and the second element is
        a reference to a sql command that will generate that table.
    '''
    table_queries = []
    for line in open(sql_sequence_file):
        # we're using -- at the start of a line as a comment indicator
        if len(line.strip()) == 0 or line.startswith("--"):
            continue
        table_name, query_file, num_tables_str = line.strip().split("\t")
        num_tables = int(num_tables_str)
        query_template = open(query_file).read().strip()
        if self_match:
            # substitute all table references with TABLE1
            query_template = re.sub(r"{TABLE\d+}", "{TABLE1}", query_template)
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
    parser.add_argument("sql_sequence_file")
    parser.add_argument("--corpora", help="comma-separated list of corpora, ordered by size",
                            default="arxiv,wos,ds,mag")
    parser.add_argument("--self_match", action="store_true")
    args = parser.parse_args()

    corpora = args.corpora.split(",")

    client = bigquery.Client()

    sql_sequence = get_sql_sequence(args.sql_sequence_file, args.source_dataset_name, corpora, args.self_match)
    mk_tables(client, args.source_dataset_name, sql_sequence)

