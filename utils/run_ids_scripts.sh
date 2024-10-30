cd run
gsutil rm gs://airflow-data-exchange/article_linkage/tmp/done_files/ids_are_done
python3 create_merge_ids.py --exact_match_dir exact_matches --exclude_dir unlink --ids_to_drop ids_to_drop --prev_id_mapping_dir prev_id_mapping --output_dir new_id_mappings
/snap/bin/gsutil -m cp -r new_id_mappings gs://airflow-data-exchange/article_linkage/tmp/
touch ids_are_done
gsutil cp ids_are_done gs://airflow-data-exchange/article_linkage/tmp/done_files/
