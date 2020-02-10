SELECT
  ids.id,
  a.year,
  b.title,
  c.abstract AS abstract,
  d.last_names AS last_names
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
