-- prepare input for LID (and downstream, for the combined metadata table)
SELECT
  id,
  title,
  abstract,
  clean_doi,
  year,
  last_names,
  references,
  dataset
FROM {{ staging_dataset }}.all_metadata_norm
