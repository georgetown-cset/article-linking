from google.cloud import bigquery

def create_dataset(dataset_name, client):
    dataset = bigquery.Dataset("gcp-cset-projects."+dataset_name)
    dataset.location = "US"
    client.create_dataset(dataset)

def mk_tables(client, dataset_name, table_queries: list):
    for table_name, query in table_queries:
        try:
            print(f"Running '{query}' for output to {dataset_name}.{table_name}")
            job_config = bigquery.QueryJobConfig(destination="gcp-cset-projects."+dataset_name+"."+table_name)
            query_job = client.query(query, job_config=job_config)
            query_job.result()
        except Exception as e:
            print(e)
