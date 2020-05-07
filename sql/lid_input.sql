-- prepare input for LID (and downstream, for the combined metadata table)
select id, title, abstract, clean_doi, year, last_names, references, dataset from {{params.dataset}}.all_metadata_norm
