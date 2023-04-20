gsutil cp linkage_dag_v3_2way_match.py gs://us-east1-production2023-cc1-01d75926-bucket/dags/
gsutil cp scholarly_lit_trigger.py gs://us-east1-production2023-cc1-01d75926-bucket/dags/
gsutil -m cp sql/* gs://us-east1-production2023-cc1-01d75926-bucket/dags/sql/article_linkage/
gsutil -m cp sequences/* gs://us-east1-production2023-cc1-01d75926-bucket/dags/sequences/article_linkage/
gsutil cp schemas/* gs://airflow-data-exchange/article_linkage/schemas/
gsutil -m cp schemas/* gs://us-east1-production2023-cc1-01d75926-bucket/dags/schemas/article_linkage/
gsutil cp utils/create_merge_ids.py gs://airflow-data-exchange/article_linkage/vm_scripts/
gsutil cp utils/run_simhash.py gs://airflow-data-exchange/article_linkage/vm_scripts/
gsutil cp utils/my_simhash.py gs://airflow-data-exchange/article_linkage/vm_scripts/
gsutil cp utils/article_linkage_lid_dataflow_requirements.txt gs://us-east1-production2023-cc1-01d75926-bucket/dags/requirements/article_linkage_lid_dataflow_requirements.txt
gsutil cp utils/article_linkage_text_clean_requirements.txt gs://us-east1-production2023-cc1-01d75926-bucket/dags/requirements/article_linkage_text_clean_requirements.txt
gsutil cp utils/clean_corpus.py gs://us-east1-production2023-cc1-01d75926-bucket/dags/linkage_scripts/
gsutil cp utils/run_lid.py gs://us-east1-production2023-cc1-01d75926-bucket/dags/linkage_scripts/
