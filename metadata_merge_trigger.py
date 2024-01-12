from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dataloader.airflow_utils.slack import task_fail_slack_alert

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 5),
    "email": ["jennifer.melot@georgetown.edu"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

with DAG(
    "org_er_and_metadata_merge",
    default_args=default_args,
    description="Triggers Org ER and metadata merge dags",
    schedule_interval=None,
    catchup=False,
) as dag:

    trigger_orgfixes1 = TriggerDagRunOperator(
        task_id="trigger_orgfixes1",
        trigger_dag_id="org_fixes",
        wait_for_completion=True,
    )
    trigger_bulk_org_er_updater = TriggerDagRunOperator(
        task_id="trigger_bulk_org_er_updater",
        trigger_dag_id="bulk_org_er_updater",
        wait_for_completion=True,
    )
    trigger_orgfixes2 = TriggerDagRunOperator(
        task_id="trigger_orgfixes2",
        trigger_dag_id="org_fixes",
        wait_for_completion=True,
    )
    trigger_merge = TriggerDagRunOperator(
        task_id="trigger_merged_article_metadata_updater",
        trigger_dag_id="merged_article_metadata_updater",
        wait_for_completion=True,
    )

    (
        trigger_orgfixes1
        >> trigger_bulk_org_er_updater
        >> trigger_orgfixes2
        >> trigger_merge
    )
