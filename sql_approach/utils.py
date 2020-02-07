from google.cloud import bigquery

def create_dataset(dataset_name, client):
    dataset = bigquery.Dataset("gcp-cset-projects."+dataset_name)
    dataset.location = "US"
    client.create_dataset(dataset)