gsutil cp linkage_dag.py gs://us-east1-etl-05d83c6e-bucket/dags/
gsutil -m cp sql/* gs://us-east1-etl-05d83c6e-bucket/dags/sql/article_linkage/
gsutil -m cp sequences/* gs://us-east1-etl-05d83c6e-bucket/dags/sequences/article_linkage/
gsutil cp schemas/* gs://airflow-data-exchange/article_linkage/schemas/
gsutil cp utils/create_merge_ids.py gs://airflow-data-exchange/article_linkage/vm_scripts/
gsutil cp utils/run_simhash.py gs://airflow-data-exchange/article_linkage/vm_scripts/
gsutil cp utils/run_simhash.py gs://airflow-data-exchange/article_linkage/vm_scripts/
gsutil cp utils/my_simhash.py gs://airflow-data-exchange/article_linkage/vm_scripts/
gsutil cp utils/article_linkage_lid_dataflow_requirements.txt gs://us-east1-etl-05d83c6e-bucket/dags/requirements/
gsutil cp utils/article_linkage_text_clean_requirements.txt gs://us-east1-etl-05d83c6e-bucket/dags/requirements/
gsutil cp utils/clean_corpus.py gs://us-east1-etl-05d83c6e-bucket/dags/linkage_scripts/
gsutil cp utils/run_lid.py gs://us-east1-etl-05d83c6e-bucket/dags/linkage_scripts/
