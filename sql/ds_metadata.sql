SELECT
  d.id,
  d.title,
  d.abstract,
  lower(d.doi) as clean_doi,
  d.year,
  a.last_names,
  array_to_string(array(select r from unnest(d.references) as r order by r), ",") as references
FROM
  gcp_cset_digital_science.dimensions_publications_with_abstracts_latest d
INNER JOIN
  {DATASET}.ds_authors a
ON
  d.id = a.id
