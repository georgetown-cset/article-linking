python3 create_merge_ids.py --match_dir usable_ids --prev_id_mapping_dir prev_id_mapping --merge_file id_mapping.jsonl --current_ids_dir article_pairs
/snap/bin/gsutil -m cp id_mapping.jsonl gs://airflow-data-exchange/article_linkage/tmp/
/snap/bin/gsutil -m cp simhash_results/* gs://airflow-data-exchange/article_linkage/simhash_results/
/snap/bin/gsutil -m cp new_simhash_indexes/* gs://airflow-data-exchange/article_linkage/simhash_indexes/
/snap/bin/gsutil -m cp new_simhash_indexes/* gs://airflow-data-exchange/article_linkage/simhash_indexes_archive/$(date +%F)/
touch ids_are_done
gsutil cp ids_are_done gs://airflow-data-exchange/article_linkage/tmp/done_files/