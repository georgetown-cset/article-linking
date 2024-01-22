import json
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowCreatePythonJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from dataloader.airflow_utils.defaults import (
    DAGS_DIR,
    DATA_BUCKET,
    GCP_ZONE,
    PROJECT_ID,
    get_default_args,
    get_post_success,
)
from dataloader.scripts.populate_documentation import update_table_descriptions

production_dataset = "literature"
staging_dataset = f"staging_{production_dataset}"

with DAG(
    "article_linkage_updater",
    default_args=get_default_args(),
    description="Links articles across our scholarly lit holdings.",
    schedule_interval=None,
    user_defined_macros={
        "staging_dataset": staging_dataset,
        "production_dataset": production_dataset,
    },
) as dag:
    bucket = DATA_BUCKET
    gcs_folder = "article_linkage"
    tmp_dir = f"{gcs_folder}/tmp"
    raw_data_dir = f"{gcs_folder}/data"
    schema_dir = f"{gcs_folder}/schemas"
    sql_dir = f"sql/{gcs_folder}"
    backup_dataset = production_dataset + "_backups"
    project_id = PROJECT_ID
    gce_zone = GCP_ZONE
    gce_resource_id = "godzilla-of-article-linkage"

    # We keep several intermediate outputs in a tmp dir on gcs, so clean it out at the start of each run. We clean at
    # the start of the run so if the run fails we can examine the failed data
    clear_tmp_dir = GCSDeleteObjectsOperator(
        task_id="clear_tmp_gcs_dir", bucket_name=bucket, prefix=tmp_dir + "/"
    )

    # Next, we'll run a different set of queries for each dataset to convert the metadata we use in the match to a
    # standard format
    metadata_sequences_start = []
    metadata_sequences_end = []
    for dataset in ["arxiv", "wos", "papers_with_code", "openalex", "s2", "lens"]:
        ds_commands = []
        query_list = [
            t.strip()
            for t in open(
                f"{DAGS_DIR}/sequences/" f"{gcs_folder}/generate_{dataset}_metadata.tsv"
            )
        ]
        # run the queries needed to generate the metadata tables
        for query_name in query_list:
            ds_commands.append(
                BigQueryInsertJobOperator(
                    task_id=query_name,
                    configuration={
                        "query": {
                            "query": "{% include '"
                            + f"{sql_dir}/{query_name}.sql"
                            + "' %}",
                            "useLegacySql": False,
                            "destinationTable": {
                                "projectId": project_id,
                                "datasetId": staging_dataset,
                                "tableId": query_name,
                            },
                            "allowLargeResults": True,
                            "createDisposition": "CREATE_IF_NEEDED",
                            "writeDisposition": "WRITE_TRUNCATE",
                        }
                    },
                )
            )
        start = ds_commands[0]
        curr = ds_commands[0]
        for c in ds_commands[1:]:
            curr >> c
            curr = c
        metadata_sequences_end.append(curr)
        metadata_sequences_start.append(start)

    # check that the ids are unique across corpora
    union_ids = BigQueryInsertJobOperator(
        task_id="union_ids",
        configuration={
            "query": {
                "query": "{% include '" + f"{sql_dir}/union_ids.sql" + "' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": staging_dataset,
                    "tableId": "union_ids",
                },
                "allowLargeResults": True,
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    check_unique_input_ids = BigQueryCheckOperator(
        task_id="check_unique_input_ids",
        sql=(
            f"select count(distinct(id)) = count(id) from {staging_dataset}.union_ids"
        ),
        use_legacy_sql=False,
    )

    # We now take the union of all the metadata and export it to GCS for normalization via Dataflow. We then run
    # the Dataflow job, and import the outputs back into BQ
    union_metadata = BigQueryInsertJobOperator(
        task_id="union_metadata",
        configuration={
            "query": {
                "query": "{% include '" + f"{sql_dir}/union_metadata.sql" + "' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": staging_dataset,
                    "tableId": "union_metadata",
                },
                "allowLargeResults": True,
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    export_metadata = BigQueryToGCSOperator(
        task_id="export_metadata",
        source_project_dataset_table=f"{staging_dataset}.union_metadata",
        destination_cloud_storage_uris=f"gs://{bucket}/{tmp_dir}/union_meta/union*.jsonl",
        export_format="NEWLINE_DELIMITED_JSON",
    )

    dataflow_options = {
        "project": "gcp-cset-projects",
        "runner": "DataflowRunner",
        "disk_size_gb": "30",
        "max_num_workers": "100",
        "region": "us-east1",
        "temp_location": f"gs://{bucket}/{tmp_dir}/clean_dataflow",
        "save_main_session": True,
        "requirements_file": f"{DAGS_DIR}/requirements/article_linkage_text_clean_requirements.txt",
    }
    clean_corpus = DataflowCreatePythonJobOperator(
        py_file=f"{DAGS_DIR}/linkage_scripts/clean_corpus.py",
        job_name="article_linkage_clean_corpus",
        task_id="clean_corpus",
        dataflow_default_options=dataflow_options,
        options={
            "input_dir": f"gs://{bucket}/{tmp_dir}/union_meta/union*",
            "output_dir": f"gs://{bucket}/{tmp_dir}/cleaned_meta/clean",
            "fields_to_clean": "title,abstract,last_names",
            "region": "us-east1",
        },
    )

    import_clean_metadata = GCSToBigQueryOperator(
        task_id="import_clean_metadata",
        bucket=bucket,
        source_objects=[f"{tmp_dir}/cleaned_meta/clean*"],
        schema_object=f"{schema_dir}/all_metadata_norm.json",
        destination_project_dataset_table=f"{staging_dataset}.all_metadata_norm",
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    filter_norm_metadata = BigQueryInsertJobOperator(
        task_id="filter_norm_metadata",
        configuration={
            "query": {
                "query": "{% include '"
                + f"{sql_dir}/all_metadata_norm_filt.sql"
                + "' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": staging_dataset,
                    "tableId": "all_metadata_norm_filt",
                },
                "allowLargeResults": True,
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    # It's now time to create the match pairs that can be found using combinations of one "strong" indicator
    # and one other indicator
    strong_indicators = ["title_norm", "abstract_norm", "clean_doi", "references"]
    weak_indicators = ["year", "last_names_norm"]
    combine_queries = []
    combine_tables = []
    for strong in strong_indicators:
        for other in strong_indicators + weak_indicators:
            if strong == other:
                continue
            table_name = f"{strong}_{other}"
            combine_tables.append(table_name)
            additional_checks = ""
            if other != "year":
                additional_checks += f' and (a.{other} != "")'
            if "references" in [strong, other]:
                additional_checks += ' and array_length(split(a.references, ",")) > 2'
            combine_queries.append(
                BigQueryInsertJobOperator(
                    task_id=table_name,
                    configuration={
                        "query": {
                            "query": "{% include '"
                            + f"{sql_dir}/match_template.sql"
                            + "' %}",
                            "useLegacySql": False,
                            "destinationTable": {
                                "projectId": project_id,
                                "datasetId": staging_dataset,
                                "tableId": table_name,
                            },
                            "allowLargeResults": True,
                            "createDisposition": "CREATE_IF_NEEDED",
                            "writeDisposition": "WRITE_TRUNCATE",
                        }
                    },
                    params={
                        "strong": strong,
                        "other": other,
                        "additional_checks": additional_checks,
                    },
                )
            )

    wait_for_combine = DummyOperator(task_id="wait_for_combine")

    merge_combine_query_list = [
        t.strip()
        for t in open(
            f"{DAGS_DIR}/sequences/" f"{gcs_folder}/merge_combined_metadata.tsv"
        )
    ]
    last_combination_query = wait_for_combine
    meta_match_queries = "\nunion all\n".join(
        [
            f"select all1_id, all2_id from {staging_dataset}.{table}\nunion all\nselect all2_id as all1_id, all1_id as all2_id from {staging_dataset}.{table}"
            for table in combine_tables
        ]
    )
    for query_name in merge_combine_query_list:
        next = BigQueryInsertJobOperator(
            task_id=query_name,
            configuration={
                "query": {
                    "query": "{% include '" + f"{sql_dir}/{query_name}.sql" + "' %}",
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": project_id,
                        "datasetId": staging_dataset,
                        "tableId": query_name,
                    },
                    "allowLargeResults": True,
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
            params={"tables": meta_match_queries},
        )
        last_combination_query >> next
        last_combination_query = next

    # Now, we need to prep some inputs for RAM and CPU-intensive code that will run on "godzilla of article linkage".
    heavy_compute_inputs = [
        BigQueryToGCSOperator(
            task_id="export_old_cset_ids",
            source_project_dataset_table=f"{production_dataset}.sources",
            destination_cloud_storage_uris=f"gs://{bucket}/{tmp_dir}/prev_id_mapping/prev_id_mapping*.jsonl",
            export_format="NEWLINE_DELIMITED_JSON",
        ),
        BigQueryToGCSOperator(
            task_id="export_article_pairs",
            source_project_dataset_table=f"{staging_dataset}.all_match_pairs_with_um",
            destination_cloud_storage_uris=f"gs://{bucket}/{tmp_dir}/article_pairs/article_pairs*.jsonl",
            export_format="NEWLINE_DELIMITED_JSON",
        ),
        BigQueryToGCSOperator(
            task_id="export_simhash_input",
            source_project_dataset_table=f"{staging_dataset}.simhash_input",
            destination_cloud_storage_uris=f"gs://{bucket}/{tmp_dir}/simhash_input/simhash_input*.jsonl",
            export_format="NEWLINE_DELIMITED_JSON",
        ),
        BigQueryToGCSOperator(
            task_id="export_lid_input",
            source_project_dataset_table=f"{staging_dataset}.lid_input",
            destination_cloud_storage_uris=f"gs://{bucket}/{tmp_dir}/lid_input/lid_input*.jsonl",
            export_format="NEWLINE_DELIMITED_JSON",
        ),
    ]

    # Start up godzilla of article linkage, update simhash indexes of title+abstract, run simhash, then create the
    # merge ids
    gce_instance_start = ComputeEngineStartInstanceOperator(
        project_id=project_id,
        zone=gce_zone,
        resource_id=gce_resource_id,
        task_id="start-" + gce_resource_id,
    )

    prep_environment_script_sequence = [
        f"/snap/bin/gsutil cp gs://{bucket}/{gcs_folder}/vm_scripts/*.sh .",
        "cd /mnt/disks/data",
        "rm -rf run",
        "mkdir run",
        "cd run",
        f"/snap/bin/gsutil cp gs://{bucket}/{gcs_folder}/vm_scripts/* .",
        "rm -rf input_data",
        "rm -rf current_ids",
        "mkdir input_data",
        "mkdir current_ids",
        f"/snap/bin/gsutil -m cp -r gs://{bucket}/{tmp_dir}/article_pairs .",
        f"/snap/bin/gsutil -m cp -r gs://{bucket}/{tmp_dir}/simhash_input .",
        f"/snap/bin/gsutil -m cp -r gs://{bucket}/{gcs_folder}/simhash_indexes .",
        f"/snap/bin/gsutil -m cp -r gs://{bucket}/{gcs_folder}/simhash_results .",
        f"/snap/bin/gsutil -m cp -r gs://{bucket}/{tmp_dir}/prev_id_mapping .",
        "mkdir new_simhash_indexes",
    ]
    prep_environment_vm_script = " && ".join(prep_environment_script_sequence)

    prep_environment = BashOperator(
        task_id="prep_environment",
        bash_command=f'gcloud compute ssh jm3312@{gce_resource_id} --zone {gce_zone} --command "{prep_environment_vm_script}"',
    )

    update_simhash_index = BashOperator(
        task_id="update_simhash_index",
        bash_command=f'gcloud compute ssh jm3312@{gce_resource_id} --zone {gce_zone} --command "bash run_simhash_scripts.sh &> log &"',
    )

    wait_for_simhash_index = GCSObjectExistenceSensor(
        task_id="wait_for_simhash_index",
        bucket=DATA_BUCKET,
        object=f"{tmp_dir}/done_files/simhash_is_done",
        deferrable=True
    )

    create_cset_ids = BashOperator(
        task_id="create_cset_ids",
        bash_command=f'gcloud compute ssh jm3312@{gce_resource_id} --zone {gce_zone} --command "bash run_ids_scripts.sh &> log &"',
    )

    wait_for_cset_ids = GCSObjectExistenceSensor(
        task_id="wait_for_cset_ids",
        bucket=DATA_BUCKET,
        object=f"{tmp_dir}/done_files/ids_are_done",
        deferrable=True
    )

    push_to_gcs = BashOperator(
        task_id="push_to_gcs",
        bash_command=f'gcloud compute ssh jm3312@{gce_resource_id} --zone {gce_zone} --command "run_ids_script.sh &"',
    )

    # while the carticle ids are updating, run lid on the titles and abstracts
    lid_dataflow_options = {
        "project": project_id,
        "runner": "DataflowRunner",
        "disk_size_gb": "30",
        "max_num_workers": "100",
        "region": "us-east1",
        "temp_location": f"gs://{bucket}/{tmp_dir}/run_lid",
        "save_main_session": True,
        "requirements_file": f"{DAGS_DIR}/requirements/article_linkage_lid_dataflow_requirements.txt",
    }
    run_lid = DataflowCreatePythonJobOperator(
        py_file=f"{DAGS_DIR}/linkage_scripts/run_lid.py",
        job_name="article_linkage_lid",
        task_id="run_lid",
        dataflow_default_options=lid_dataflow_options,
        options={
            "input_dir": f"gs://{bucket}/{tmp_dir}/lid_input/lid_input*",
            "output_dir": f"gs://{bucket}/{tmp_dir}/lid_output/lid",
            "fields_to_lid": "title,abstract",
            "region": "us-east1",
        },
    )

    # turn off the expensive godzilla of article linkage when we're done with it, then import the id mappings and
    # lid back into BQ
    gce_instance_stop = ComputeEngineStopInstanceOperator(
        project_id=project_id,
        zone=gce_zone,
        resource_id=gce_resource_id,
        task_id="stop-" + gce_resource_id,
    )

    import_id_mapping = GCSToBigQueryOperator(
        task_id="import_id_mapping",
        bucket=bucket,
        source_objects=[f"{tmp_dir}/id_mapping.jsonl"],
        schema_object=f"{schema_dir}/id_mapping.json",
        destination_project_dataset_table=f"{staging_dataset}.id_mapping",
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    import_lid = GCSToBigQueryOperator(
        task_id="import_lid",
        bucket=bucket,
        source_objects=[f"{tmp_dir}/lid_output/lid*"],
        schema_object=f"{schema_dir}/all_metadata_with_cld2_lid.json",
        destination_project_dataset_table=f"{staging_dataset}.all_metadata_with_cld2_lid",
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    # generate the rest of the tables that will be copied to the production dataset
    start_final_transform_queries = DummyOperator(task_id="start_final_transform")

    final_transform_queries = [
        t.strip()
        for t in open(
            f"{DAGS_DIR}/sequences/" f"{gcs_folder}/generate_merged_metadata.tsv"
        )
    ]
    last_transform_query = start_final_transform_queries
    for query_name in final_transform_queries:
        next = BigQueryInsertJobOperator(
            task_id=query_name,
            configuration={
                "query": {
                    "query": "{% include '" + f"{sql_dir}/{query_name}.sql" + "' %}",
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": project_id,
                        "datasetId": staging_dataset,
                        "tableId": query_name,
                    },
                    "allowLargeResults": True,
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
        )
        last_transform_query >> next
        last_transform_query = next

    # we're about to copy tables from staging to production, so do checks to make sure we haven't broken anything
    # along the way
    check_queries = []
    all_metadata_table = "all_metadata_with_cld2_lid"
    staging_tables = ["sources", "references", all_metadata_table]
    production_tables = ["sources", "references"]
    for table_name in staging_tables:
        compare_table_name = (
            table_name
            if table_name != all_metadata_table
            else all_metadata_table + "_last_run"
        )
        compare_dataset = (
            production_dataset if table_name != all_metadata_table else staging_dataset
        )
        check_queries.append(
            BigQueryCheckOperator(
                task_id="check_monotonic_increase_" + table_name.lower(),
                sql=(
                    f"select (select count(0) from {staging_dataset}.{table_name}) >= "
                    f"(select 0.8*count(0) from {compare_dataset}.{compare_table_name})"
                ),
                use_legacy_sql=False,
            )
        )

    check_queries.extend(
        [
            BigQueryCheckOperator(
                task_id="check_pks_are_unique_sources",
                sql=f"select count(orig_id) = count(distinct(orig_id)) from {staging_dataset}.sources",
                use_legacy_sql=False,
            ),
            BigQueryCheckOperator(
                task_id="all_ids_survived",
                sql=(
                    f"select count(0) = 0 from (select id from {staging_dataset}.union_ids "
                    f"where id not in (select orig_id from {staging_dataset}.sources))"
                ),
                use_legacy_sql=False,
            ),
            BigQueryCheckOperator(
                task_id="all_trivial_matches_survived",
                sql=f"""
            select
              count(concat(all1_id, " ", all2_id)) = 0
            from
              {staging_dataset}.metadata_match
            where concat(all1_id, " ", all2_id) not in (
              select
                concat(links1.orig_id, " ", links2.orig_id)
              from
                {staging_dataset}.sources links1
              left join
                {staging_dataset}.sources links2
              on links1.merged_id = links2.merged_id
            )
            """,
                use_legacy_sql=False,
            ),
            BigQueryCheckOperator(
                task_id="no_null_references",
                sql=f"select count(0) = 0 from {staging_dataset}.references where merged_id is null or ref_id is null",
                use_legacy_sql=False,
            ),
            BigQueryCheckOperator(
                task_id="no_null_datasets",
                sql=f"select count(0) = 0 from {staging_dataset}.sources where dataset is null",
                use_legacy_sql=False,
            ),
        ]
    )

    # We're done! Checks passed, so copy to production and post success to slack
    start_production_cp = DummyOperator(task_id="start_production_cp")
    success_alert = get_post_success("Article linkage update succeeded!", dag)
    curr_date = datetime.now().strftime("%Y%m%d")
    with open(
        f"{os.environ.get('DAGS_FOLDER')}/schemas/{gcs_folder}/table_descriptions.json"
    ) as f:
        table_desc = json.loads(f.read())

    trigger_org_er_and_metadata_merge = TriggerDagRunOperator(
        task_id="trigger_org_er_and_metadata_merge",
        trigger_dag_id="org_er_and_metadata_merge",
    )

    for table in production_tables:
        push_to_production = BigQueryToBigQueryOperator(
            task_id="copy_" + table.lower(),
            source_project_dataset_tables=[f"{staging_dataset}.{table}"],
            destination_project_dataset_table=f"{production_dataset}.{table}",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
        )
        snapshot = BigQueryToBigQueryOperator(
            task_id=f"snapshot_{table}",
            source_project_dataset_tables=[f"{production_dataset}.{table}"],
            destination_project_dataset_table=f"{backup_dataset}.{table}_{curr_date}",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
        )
        pop_descriptions = PythonOperator(
            task_id="populate_column_documentation_for_" + table,
            op_kwargs={
                "input_schema": f"{os.environ.get('DAGS_FOLDER')}/schemas/{gcs_folder}/{table}.json",
                "table_name": f"{production_dataset}.{table}",
                "table_description": table_desc[table],
            },
            python_callable=update_table_descriptions,
        )
        (
            start_production_cp
            >> push_to_production
            >> snapshot
            >> pop_descriptions
            >> success_alert
            >> trigger_org_er_and_metadata_merge
        )

    # We don't show the "all metadata" table in the production dataset, but we do need to
    # be able to diff the current data from the data used in the last run in simhash_input
    copy_cld2 = BigQueryToBigQueryOperator(
        task_id=f"copy_{all_metadata_table}",
        source_project_dataset_tables=[f"{staging_dataset}.{all_metadata_table}"],
        destination_project_dataset_table=f"{staging_dataset}.{all_metadata_table}_last_run",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    snapshot_cld2 = BigQueryToBigQueryOperator(
        task_id=f"snapshot_{all_metadata_table}",
        source_project_dataset_tables=[f"{staging_dataset}.{all_metadata_table}"],
        destination_project_dataset_table=f"{backup_dataset}.{all_metadata_table}_{curr_date}",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )
    start_production_cp >> copy_cld2 >> snapshot_cld2 >> success_alert

    # task structure
    clear_tmp_dir >> metadata_sequences_start
    (
        metadata_sequences_end
        >> union_ids
        >> check_unique_input_ids
        >> union_metadata
        >> export_metadata
        >> clean_corpus
        >> import_clean_metadata
        >> filter_norm_metadata
        >> combine_queries
        >> wait_for_combine
    )

    (last_combination_query >> heavy_compute_inputs >> gce_instance_start >> prep_environment >>
     update_simhash_index >> wait_for_simhash_index >> create_cset_ids >> wait_for_cset_ids >> push_to_gcs >>
     gce_instance_stop)

    gce_instance_start >> run_lid >> gce_instance_stop

    gce_instance_stop >> [import_id_mapping, import_lid] >> start_final_transform_queries

    last_transform_query >> check_queries >> start_production_cp
