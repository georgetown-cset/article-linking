SELECT
  ids.id,
  a.year,
  b.title,
  c.abstract AS abstract,
  d.last_name AS last_names
FROM
  {DATASET}.wos_ids ids
LEFT JOIN
  {DATASET}.wos_unique_pubyears a
ON
  ids.id = a.id
LEFT JOIN
  {DATASET}.wos_titles b
ON
  ids.id = b.id
LEFT JOIN
  {DATASET}.wos_abstract_paragraphs c
ON
  ids.id = c.id
LEFT JOIN
  wos_dim.wos_authors d
ON
  ids.id = d.id