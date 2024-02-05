from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dataloader.airflow_utils.defaults import get_default_args


with DAG(
    "org_er_and_metadata_merge",
    default_args=get_default_args(pocs=["Jennifer"]),
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
