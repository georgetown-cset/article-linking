gsutil cp linkage_dag.py gs://us-east1-etl-05d83c6e-bucket/dags/
gsutil cp utils/clean_corpus.py gs://us-east1-etl-05d83c6e-bucket/dags/linkage_scripts/
gsutil cp utils/run_lid.py gs://us-east1-etl-05d83c6e-bucket/dags/linkage_scripts/
gsutil -m cp sql/* gs://us-east1-etl-05d83c6e-bucket/dags/sql/article_linkage/
gsutil cp schemas/metadata.json gs://airflow-data-exchange/article_linkage/schemas/
gsutil cp schemas/links_table.json gs://airflow-data-exchange/article_linkage/schemas/
gsutil cp schemas/all_metadata_schema_cld2.json gs://airflow-data-exchange/article_linkage/schemas/
gsutil cp create_merge_ids.py gs://airflow-data-exchange/article_linkage/vm_scripts/
gsutil cp utils/run_simhash.py gs://airflow-data-exchange/article_linkage/vm_scripts/
gsutil cp utils/article_linkage_lid_dataflow_requirements.txt gs://us-east1-etl-05d83c6e-bucket/dags/requirements/
gsutil cp article_linkage_text_clean_requirements.txt gs://us-east1-etl-05d83c6e-bucket/dags/requirements/
