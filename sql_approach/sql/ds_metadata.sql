SELECT
  d.id,
  d.year,
  d.title,
  d.abstract,
  a.last_name
FROM
  gcp_cset_digital_science.dimensions_publications_with_abstracts_latest d
INNER JOIN
  {DATASET}.ds_authors a
ON
  d.id = a.id