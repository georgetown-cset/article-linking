SELECT
  ids.id,
  b.title,
  c.abstract,
  f.clean_doi,
  a.year,
  d.last_names,
  e.references
FROM
  {DATASET}.wos_ids ids
LEFT JOIN
  {DATASET}.wos_pubyears a
ON
  ids.id = a.id
LEFT JOIN
  {DATASET}.wos_titles b
ON
  ids.id = b.id
LEFT JOIN
  {DATASET}.wos_abstracts c
ON
  ids.id = c.id
LEFT JOIN
  {DATASET}.wos_authors d
ON
  ids.id = d.id
LEFT JOIN
  (select id, string_agg(ref_id order by ref_id) as references
  from gcp_cset_clarivate.wos_references_latest group by id) e
ON
  ids.id = e.id
LEFT JOIN
  (select id, lower(identifier_value) as clean_doi from gcp_cset_clarivate.wos_dynamic_identifiers_latest where
    (identifier_type="doi") and (identifier_value is not null)) f
ON
  ids.id = f.id