-- prepare wos metadata for standard form used in match
SELECT
  ids.id,
  b.title,
  c.abstract,
  f.clean_doi,
  a.year,
  d.last_names,
  e.references
FROM
  {{ staging_dataset }}.wos_ids AS ids
LEFT JOIN
  {{ staging_dataset }}.wos_pubyears AS a
  ON
    ids.id = a.id
LEFT JOIN
  {{ staging_dataset }}.wos_titles AS b
  ON
    ids.id = b.id
LEFT JOIN
  {{ staging_dataset }}.wos_abstracts AS c
  ON
    ids.id = c.id
LEFT JOIN
  {{ staging_dataset }}.wos_authors AS d
  ON
    ids.id = d.id
LEFT JOIN
  (SELECT
    id,
    string_agg(ref_id ORDER BY ref_id) AS references --noqa: L029
    FROM gcp_cset_clarivate.wos_references_latest GROUP BY id) AS e
  ON
    ids.id = e.id
LEFT JOIN
  (SELECT
    id,
    lower(identifier_value) AS clean_doi
    FROM gcp_cset_clarivate.wos_dynamic_identifiers_latest WHERE
      (identifier_type = "doi") AND (identifier_value IS NOT NULL)) AS f
  ON
    ids.id = f.id
