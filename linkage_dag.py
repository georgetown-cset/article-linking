import os

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.gcp_compute_operator import GceInstanceStartOperator, GceInstanceStopOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.operators.bash_operator import BashOperator
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
    "on_failure_callback": task_fail_slack_alert
}

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
    production_dataset = "gcp_cset_links_v2"
    project_id = "gcp-cset-projects"
    gce_zone = "us-east1-c"
    gce_resource_id = "godzilla-of-article-linkage"

    clear_tmp_dir = GoogleCloudStorageDeleteOperator(
        task_id="clear_tmp_gcs_dir",
        bucket_name=bucket,
        prefix=tmp_dir + "/"
    )

    metadata_sequences_start = []
    metadata_sequences_end = []
    for dataset in ["arxiv", "cnki", "ds", "mag", "wos"]:
        ds_commands = []
        query_list = [t.strip() for t in open(f"{os.environ.get('DAGS_FOLDER')}/sequences/"
                                                           f"{gcs_folder}/generate_{dataset}_metadata.tsv")]
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
        destination_cloud_storage_uris=f"gs://{bucket}/{tmp_dir}/union_meta/union*.jsonl",
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
        "requirements_file": f"{os.environ.get('DAGS_FOLDER')}/requirements/article_linkage_text_clean_requirements.txt"
    }
    clean_corpus = DataFlowPythonOperator(
        py_file=f"{os.environ.get('DAGS_FOLDER')}/linkage_scripts/clean_corpus.py",
        job_name="article_linkage_clean_corpus",
        task_id="clean_corpus",
        dataflow_default_options=dataflow_options,
        options={
            "input_dir": f"gs://{bucket}/{tmp_dir}/union_meta/union*",
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

    combine_commands = []
    query_list = [t.strip() for t in open(f"{os.environ.get('DAGS_FOLDER')}/sequences/"
                                                           f"{gcs_folder}/combine_metadata.tsv")]
    # todo: parallelize, these don't all need to run in series
    for query_name in query_list:
        combine_commands.append(BigQueryOperator(
            task_id=query_name,
            sql=f"{sql_dir}/{query_name}.sql",
            params={
                "dataset": staging_dataset,
                "staging_dataset": staging_dataset,
                "production_dataset": production_dataset
            },
            destination_dataset_table=f"{staging_dataset}.{query_name}",
            allow_large_results=True,
            use_legacy_sql=False,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE"
        ))

    heavy_compute_inputs = [
        BigQueryToCloudStorageOperator(
            task_id="export_old_cset_ids",
            source_project_dataset_table=f"{production_dataset}.article_links",
            destination_cloud_storage_uris=f"gs://{bucket}/{tmp_dir}/prev_id_mapping/prev_id_mapping*.jsonl",
            export_format="NEWLINE_DELIMITED_JSON"
        ),
        BigQueryToCloudStorageOperator(
            task_id="export_article_pairs",
            source_project_dataset_table=f"{staging_dataset}.all_match_pairs_with_um",
            destination_cloud_storage_uris=f"gs://{bucket}/{tmp_dir}/article_pairs/article_pairs*.jsonl",
            export_format="NEWLINE_DELIMITED_JSON"
        ),
        BigQueryToCloudStorageOperator(
            task_id="export_simhash_input",
            source_project_dataset_table=f"{staging_dataset}.simhash_input",
            destination_cloud_storage_uris=f"gs://{bucket}/{tmp_dir}/simhash_input/simhash_input*.jsonl",
            export_format="NEWLINE_DELIMITED_JSON"
        ),
        BigQueryToCloudStorageOperator(
            task_id="export_lid_input",
            source_project_dataset_table=f"{staging_dataset}.lid_input",
            destination_cloud_storage_uris=f"gs://{bucket}/{tmp_dir}/lid_input/lid_input*.jsonl",
            export_format="NEWLINE_DELIMITED_JSON"
        )
    ]

    #### run time and ram-intensive scripts on a powerful VM. Meanwhile, run a dataflow job to do LID on the article
    #### titles and abstracts
    gce_instance_start = GceInstanceStartOperator(
        project_id=project_id,
        zone=gce_zone,
        resource_id=gce_resource_id,
        task_id="start-"+gce_resource_id
    )

    vm_script_sequence = [
        "cd /mnt/disks/data",
        "rm -rf run",
        "cd run",
        "gsutil cp gs://{bucket}/{gcs_folder}/vm_scripts/* .",
        "rm -rf input_data",
        "rm -rf current_ids",
        "mkdir input_data",
        "mkdir current_ids",
        f"gsutil -m cp -r gs://{bucket}/{tmp_dir}/article_pairs .",
        f"gsutil -m cp -r gs://{bucket}/{tmp_dir}/simhash_input .",
        f"gsutil -m cp -r gs://{bucket}/{gcs_folder}/simhash_indexes .",
        f"gsutil -m cp -r gs://{bucket}/{gcs_folder}/simhash_results .",
        f"gsutil -m cp -r gs://{bucket}/{tmp_dir}/prev_id_mapping .",
        "mkdir new_simhash_indexes",
        "python3 run_simhash.py simhash_input simhash_results --simhash_indexes simhash_indexes --new_simhash_indexes new_simhash_indexes",
        "cp simhash_results/* article_pairs/",
        "python3 create_merge_ids.py --match_dir article_pairs --prev_id_mapping_dir prev_id_mapping --merge_file id_mapping.jsonl",
        f"gsutil -m cp id_mapping.jsonl gs://{bucket}/{gcs_folder}/tmp/",
        f"gsutil rm -r gs://{bucket}/{gcs_folder}/simhash_results",
        f"gsutil -m cp -r simhash_results gs://{bucket}/{gcs_folder}/",
        f"gsutil rm -r gs://{bucket}/{gcs_folder}/simhash_indexes",
        f"gsutil -m cp -r new_simhash_indexes gs://{bucket}/{gcs_folder}/simhash_indexes"
    ]
    vm_script = ";".join(vm_script_sequence)

    create_cset_ids = BashOperator(
        task_id="create_cset_ids",
        bash_command=f"gcloud compute ssh {gce_resource_id} --zone {gce_zone} --command \"{vm_script}\""
    )


    lid_dataflow_options = {
        "project": project_id,
        "runner": "DataflowRunner",
        "disk_size_gb": "30",
        "max_num_workers": "100",
        "region": "us-east1",
        "temp_location": "gs://cset-dataflow-test/example-tmps/",
        "save_main_session": "",
        "requirements_file": f"{os.environ.get('DAGS_FOLDER')}/requirements/article_linkage_lid_dataflow_requirements.txt"
    }
    run_lid = DataFlowPythonOperator(
        py_file=f"{os.environ.get('DAGS_FOLDER')}/linkage_scripts/run_lid.py",
        job_name="article_linkage_lid",
        task_id="run_lid",
        dataflow_default_options=lid_dataflow_options,
        options={
            "input_dir": f"gs://{bucket}/{tmp_dir}/lid_input/lid_input*",
            "output_dir": f"gs://{bucket}/{tmp_dir}/lid_output/lid",
            "fields_to_lid": "title,abstract"
        },
    )

    gce_instance_stop = GceInstanceStopOperator(
        project_id=project_id,
        zone=gce_zone,
        resource_id=gce_resource_id,
        task_id="stop-"+gce_resource_id
    )

    import_id_mapping = GoogleCloudStorageToBigQueryOperator(
        task_id="import_id_mapping",
        bucket=bucket,
        source_objects=[f"{tmp_dir}/id_mapping.jsonl"],
        schema_object=f"{schema_dir}/links_table.json",
        destination_project_dataset_table=f"{staging_dataset}.article_links",
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE"
    )

    import_lid = GoogleCloudStorageToBigQueryOperator(
        task_id="import_lid",
        bucket=bucket,
        source_objects=[f"{tmp_dir}/lid_output/lid*"],
        schema_object=f"{schema_dir}/all_metadata_schema_cld2.json",
        destination_project_dataset_table=f"{staging_dataset}.all_metadata_with_cld2_lid",
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE"
    )

    start_final_transform_queries = DummyOperator(task_id="start_final_transform")
    final_transform_queries = [t.strip() for t in open(f"{os.environ.get('DAGS_FOLDER')}/sequences/"
                                           f"{gcs_folder}/generate_merged_metadata.tsv")]
    last_transform_query = start_final_transform_queries
    for query_name in final_transform_queries:
        next = BigQueryOperator(
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
        )
        last_transform_query >> next
        last_transform_query = next

    check_queries = []
    production_tables = ["all_metadata_with_cld2_lid", "article_links", "article_links_with_dataset", "article_merged_meta",
                  "mapped_references"]
    for table_name in production_tables:
        check_queries.append(BigQueryCheckOperator(
            task_id="check_monotonic_increase_"+table_name.lower(),
            sql=(f"select (select count(0) from {staging_dataset}.{table_name}) >= "
                 f"(select count(0) from {production_dataset}.{table_name})"),
            use_legacy_sql=False
        ))

    # now, check that primary keys are actually unique
    for table_name, pk in [("article_links", "orig_id"), ("article_links_with_dataset", "orig_id"),
                           ("article_merged_meta", "merged_id")]:
        check_queries.append(BigQueryCheckOperator(
            task_id="check_pks_are_unique_"+table_name.lower(),
            sql=f"select count({pk}) = count(distinct({pk})) from {staging_dataset}.{table_name}",
            use_legacy_sql=False
        ))

    start_production_cp = DummyOperator(task_id="start_production_cp")

    push_to_production = []
    for table in production_tables:
        push_to_production.append(BigQueryToBigQueryOperator(
            task_id="copy_"+table.lower(),
            source_project_dataset_tables=[f"{staging_dataset}.{table}"],
            destination_project_dataset_table=f"{production_dataset}.{table}",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE"
        ))

    success_alert = SlackWebhookOperator(
        task_id="post_success",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message="Article linkage update succeeded!",
        username="airflow"
    )

    clear_tmp_dir >> metadata_sequences_start
    metadata_sequences_end >> union_metadata >> export_metadata >> clean_corpus >> import_clean_metadata
    last_combination_query = import_clean_metadata
    for c in combine_commands:
        last_combination_query >> c
        last_combination_query = c

    (last_combination_query >> heavy_compute_inputs >> gce_instance_start >> [create_cset_ids, run_lid] >>
        gce_instance_stop >> [import_id_mapping, import_lid] >> start_final_transform_queries)

    last_transform_query >> check_queries >> start_production_cp >> push_to_production >> success_alert




