import functools
import json
import os

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators import DummyOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from datetime import timedelta, datetime


from dataloader.airflow_utils.slack import task_fail_slack_alert


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 15),
    "email": ["jennifer.melot@georgetown.edu"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    #"on_failure_callback": task_fail_slack_alert
}

# looks like Sunday morning would be a safe time to kick this off given their past update schedule
with DAG("article_linkage_updater",
            default_args=default_args,
            description="Links articles across our scholarly lit holdings.",
            schedule_interval=None) as dag:
    slack_webhook_token = BaseHook.get_connection("slack").password
    bucket = "airflow-data-exchange"
    gcs_folder = "article_linkage"
    import_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tmp_dir = f"{gcs_folder}/tmp"
    raw_data_dir = f"{gcs_folder}/data"
    schema_dir = f"{gcs_folder}/schemas"
    sql_dir = f"sql/{gcs_folder}"
    staging_dataset = "staging_gcp_cset_links"

    clear_tmp_dir = GoogleCloudStorageDeleteOperator(
        task_id="clear_tmp_gcs_dir",
        bucket_name=bucket,
        prefix=tmp_dir + "/"
    )

    metadata_sequences_start = []
    metadata_sequences_end = []
    for dataset in ["arxiv", "cnki", "ds", "mag", "wos"]:
        ds_commands = []
        query_list = json.loads(Variable.get(f"article_linkage:{dataset}_metadata_queries"))
        # run the queries needed to generate the metadata tables
        for query_name in query_list:
             ds_commands.append(BigQueryOperator(
                task_id=query_name,
                sql=f"{sql_dir}/{query_name}.sql",
                params={
                    "dataset": staging_dataset
                },
                destination_dataset_table=f"{staging_dataset}.{query_name}",
                allow_large_results=True,
                use_legacy_sql=False,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_TRUNCATE"
            ))
        # this doesn't work... figure out why later
        # metadata_sequences.append(functools.reduce(BigQueryOperator.set_downstream, ds_commands))
        start = ds_commands[0]
        curr = ds_commands[0]
        for c in ds_commands[1:]:
            curr >> c
            curr = c
        metadata_sequences_end.append(curr)
        metadata_sequences_start.append(start)

    union_metadata = BigQueryOperator(
        task_id="union_metadata",
        sql=f"{sql_dir}/union_metadata.sql",
        params={
            "dataset": staging_dataset
        },
        destination_dataset_table=f"{staging_dataset}.union_metadata",
        allow_large_results=True,
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE"
    )

    export_metadata = BigQueryToCloudStorageOperator(
        task_id="export_metadata",
        source_project_dataset_table=f"{staging_dataset}.union_metadata",
        destination_cloud_storage_uris=f"gs://{bucket}/{tmp_dir}/union_meta*.jsonl",
        export_format="NEWLINE_DELIMITED_JSON"
    )

    dataflow_options = {
        "project": "gcp-cset-projects",
        "runner": "DataflowRunner",
        "disk_size_gb": "30",
        "max_num_workers": "100",
        "region": "us-east1",
        "temp_location": "gs://cset-dataflow-test/example-tmps/",
        "save_main_session": "",
        "requirements_file": f"{os.environ.get('DAGS_FOLDER')}/article_linkage_dataflow_requirements.txt"
    }
    clean_corpus = DataFlowPythonOperator(
        py_file=f"{os.environ.get('DAGS_FOLDER')}/linkage_scripts/clean_corpus.py",
        job_name="article_linkage_clean_corpus",
        task_id="clean_corpus",
        dataflow_default_options=dataflow_options,
        options={
            "input_dir": f"gs://{bucket}/{tmp_dir}/union_meta*",
            "output_dir": f"gs://{bucket}/{tmp_dir}/cleaned_meta/clean",
            "fields_to_clean": "title,abstract,last_names"
        },
    )

    import_clean_metadata = GoogleCloudStorageToBigQueryOperator(
        task_id="import_clean_metadata",
        bucket=bucket,
        source_objects=[f"{tmp_dir}/cleaned_meta/clean*"],
        schema_object=f"{schema_dir}/metadata.json",
        destination_project_dataset_table=f"{staging_dataset}.all_metadata_norm",
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE"
    )

    ds_commands = []
    query_list = json.loads(Variable.get("article_linkage:combine_metadata"))
    # run the queries needed to generate the metadata tables
    for query_name in query_list:
        ds_commands.append(BigQueryOperator(
            task_id=query_name,
            sql=f"{sql_dir}/{query_name}.sql",
            params={
                "dataset": staging_dataset
            },
            destination_dataset_table=f"{staging_dataset}.{query_name}",
            allow_large_results=True,
            use_legacy_sql=False,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE"
        ))


    clear_tmp_dir >> metadata_sequences_start
    metadata_sequences_end >> union_metadata >> export_metadata >> clean_corpus
    last_query = import_clean_metadata
    for c in ds_commands:
        last_query >> c
        last_query = c

    import_clean_metadata


