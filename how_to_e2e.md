1.) Generate the metadata tables

`python3 generate_metadata_tables.py DATASET`

2.) Export the data to GCS and run the beam normalizer

```
bq extract --destination_format NEWLINE_DELIMITED_JSON \
DATASET.TABLE gs://jtm-tmp1/DATE/pre-clean-TABLE-*
```

```
python3 clean_corpus.py gs://jtm-tmp1/DATE/pre-clean-TABLE-* gs://jtm-tmp1/DATE/post-clean-TABLE- \
title,abstract,last_names --aggressive --project gcp-cset-projects --disk_size_gb 30 --job_name clean-corpus \
--save_main_session --region us-east1 --temp_location gs://cset-dataflow-test/example-tmps/ \
--runner DataflowRunner --requirements_file dataflow-requirements.txt
```

(example: 

```
python3 clean_corpus.py gs://jtm-tmp1/2020-02-10/pre-clean-arxiv_metadata-* \
gs://jtm-tmp1/2020-02-10/post-clean-arxiv_metadata- title,abstract,last_names --aggressive \
--project gcp-cset-projects --disk_size_gb 30 --job_name clean-corpus --save_main_session \
--region us-east1 --temp_location gs://cset-dataflow-test/example-tmps/ --runner DataflowRunner \
--requirements_file dataflow-requirements.txt
```

)

```
bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect \
DATASET.TABLE_norm gs://jtm-tmp1/DATE/post-clean-TABLE-* --autodetect
```

(example:

```
bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect \
test_generate_metadata.arxiv_metadata_norm \
gs://jtm-tmp1/2020-02-10/post-clean-arxiv_metadata-*
```

)

3.) Generate the paired tables

`xxx`

This is approximately the point at which you have to start making tricky decisions. The following
pipeline would need some tweaking if you start running this over any datasets but arxiv, wos, ds, mag.
It would also need tweaking in some places if you wanted to keep the base _norm tables in a separate 
dataset. The sequence below does the following steps:

a.) Take highest-scoring pair  

b.) Generate triples

c.) Generate four-way matches

d.) Add three-way matches

e.) Add two-way matches

f.) Add one-way matches