-- find articles that match on normalized author last names, references, and doi
SELECT
  a.id AS all1_id,
  m.id AS all2_id
FROM {{ staging_dataset }}.all_metadata_norm_filt AS a
INNER JOIN
  {{ staging_dataset }}.all_metadata_norm_filt AS m
  ON ((a.last_names_norm = m.last_names_norm) AND (m.last_names_norm IS NOT NULL) AND (a.last_names_norm != "")
    AND (a.references = m.references) AND (a.references IS NOT NULL) AND (a.references != "")
    AND (a.clean_doi = m.clean_doi) AND (a.clean_doi IS NOT NULL) AND (a.clean_doi != ""))
