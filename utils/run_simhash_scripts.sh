python3 run_simhash.py simhash_input simhash_results --simhash_indexes simhash_indexes --new_simhash_indexes new_simhash_indexes
cp -r article_pairs usable_ids
cp simhash_results/* article_pairs/
touch simhash_is_done
gsutil cp simhash_is_done gs://airflow-data-exchange/article_linkage/tmp/done_files/