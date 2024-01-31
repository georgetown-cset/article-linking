gsutil cp linkage_dag.py gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/
gsutil cp metadata_merge_trigger.py gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/
gsutil rm -r gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sql/article_linkage/*
gsutil -m cp sql/* gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sql/article_linkage/
gsutil rm -r gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sequences/article_linkage/*
gsutil -m cp sequences/* gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sequences/article_linkage/
gsutil rm -r gs://airflow-data-exchange/article_linkage/schemas/*
gsutil cp schemas/* gs://airflow-data-exchange/article_linkage/schemas/
gsutil rm -r gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/schemas/article_linkage/*
gsutil -m cp schemas/* gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/schemas/article_linkage/
gsutil cp utils/* gs://airflow-data-exchange/article_linkage/vm_scripts/
gsutil cp utils/article_linkage_lid_dataflow_requirements.txt gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/requirements/article_linkage_lid_dataflow_requirements.txt
gsutil cp utils/article_linkage_text_clean_requirements.txt gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/requirements/article_linkage_text_clean_requirements.txt
gsutil cp utils/clean_corpus.py gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/linkage_scripts/
gsutil cp utils/run_lid.py gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/linkage_scripts/
