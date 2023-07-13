WITH
  dois AS (
  -- in some cases, lens provides more than one doi per article
  SELECT
    lens_id,
    LOWER(id.value) AS clean_doi
  FROM
    lens.scholarly
  CROSS JOIN
    UNNEST(external_ids) AS id
  WHERE
    id.type = "doi" ),
  author_last_names AS (
  SELECT
    lens_id,
    ARRAY_AGG(author.last_name) AS last_names
  FROM
    lens.scholarly
  CROSS JOIN
    UNNEST(authors) AS author
  WHERE
    author.last_name IS NOT NULL
  GROUP BY
    lens_id ),
  out_citations AS (
  SELECT
    scholarly.lens_id,
    STRING_AGG(reference.lens_id) AS references
  FROM
    lens.scholarly
  CROSS JOIN
    UNNEST(references) AS reference
  GROUP BY
    lens_id )
SELECT
  scholarly.lens_id AS id,
  title,
  abstract,
  clean_doi,
  year_published AS year,
  last_names,
  out_citations.references
FROM
  lens.scholarly
LEFT JOIN
  dois
USING
  (lens_id)
LEFT JOIN
  author_last_names
USING
  (lens_id)
LEFT JOIN
  out_citations
USING
  (lens_id)
WHERE
  lens_id IN (SELECT id from {{ staging_dataset }}.lens_ids)
