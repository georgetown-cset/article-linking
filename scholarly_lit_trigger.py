from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta, datetime

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
    "on_failure_callback": task_fail_slack_alert
}

with DAG("scholarly_lit_trigger",
            default_args=default_args,
            description="Triggers series of scholarly literature dags",
            schedule_interval="0 0 * * 6",
            catchup=False
         ) as dag:

    start = DummyOperator(task_id="start")

    trigger_linkage = TriggerDagRunOperator(
        task_id="trigger_article_linkage_updater",
        trigger_dag_id="article_linkage_updater",
        wait_for_completion=True
    )

    for prerequisite_dag in ["clarivate_tables_updater"]:
        trigger = TriggerDagRunOperator(
            task_id="trigger_"+prerequisite_dag,
            trigger_dag_id=prerequisite_dag,
            wait_for_completion=True
        )
        start >> trigger >> trigger_linkage

    trigger_bulk_er = TriggerDagRunOperator(
        task_id="trigger_bulk_org_er_updater",
        trigger_dag_id="bulk_org_er_updater",
        wait_for_completion=True
    )

    trigger_metadata_merge = TriggerDagRunOperator(
        task_id="trigger_merged_article_metadata_updater",
        trigger_dag_id="merged_article_metadata_updater",
        wait_for_completion=True
    )

    trigger_linkage >> trigger_bulk_er >> trigger_metadata_merge
