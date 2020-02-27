## Normalizing Article Metadata 

We are merging four datasets, all of which are structured differently in our internal database. To
match article metadata, we first need to extract the columns from this data that we want to use
in matching into a consistent set of tables with the following columns:

- id
- publication year
- title
- abstract
- author last names
- references
- doi

To do this, we run the SQL queries specified in `sequences/generate_metadata.tsv` using
[generate_tables](../generate_tables.py) (which contains some documentation on how the sequence file is
constructed and used). Mostly this is fairly straightforward, but we do note that for MAG we exclude
documents with a `DocType` of Dataset or Patent. Additionally, we take every combination of the WOS
titles, abstracts, and pubyear so that a match on any of these combinations will result in a match on
the shared WOS id.

From the project root, run: `python3 generate_tables.py <your dataset name> sequences/generate_metadata.tsv`

Having generated the metadata tables, we now need to normalize the metadata. To do this, we use 
the [clean_corpus](../utils/clean_corpus.py) script, which applies several text normalizations to the
data. 

Export the data as JSONL to GCS and then (from `utils`), run:

```
python3 clean_corpus.py gs://<path to input data> \
    gs://<path to output data> title,abstract,last_names --project gcp-cset-projects \
    --disk_size_gb 30 --job_name clean_corpus --save_main_session --region us-east1 \
    --temp_location gs://cset-dataflow-test/example-tmps/ --runner DataflowRunner \
    --requirements_file dataflow-requirements.txt 
```

Then load the resulting JSONL files into BigQuery (in future we may switch back over to the BigQuery
reader/writer for simplicity). E.g.,
 
```
bq load --autodetect --source_format NEWLINE_DELIMITED_JSON \
gs://<path to output data> dataset.all_metadata_norm
```
 
We are now ready to construct the set of matched pairs of articles.

\>> [Next Section](1_matching_table_generation.md)