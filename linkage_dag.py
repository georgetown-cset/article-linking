import functools
import json

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
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
    sql_dir = f"sql/{gcs_folder}"
    staging_dataset = "staging_gcp_cset_links"

    start_op = DummyOperator(
        task_id="start"
    )

    metadata_sequences = []
    for dataset in ["arxiv", "cnki", "ds", "mag", "wos"]:
        ds_commands = []
        query_list = json.loads(Variable.get(f"article_linkage:{dataset}_metadata_queries"))
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
        metadata_sequences.append(start)

    start_op >> metadata_sequences

    # create staging dataset

    # run metadata creation queries

    # run data cleaning script

