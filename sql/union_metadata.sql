-- glue all the metadata together into one table
WITH meta AS (
  SELECT
    cast(id AS STRING) AS id,
    title,
    abstract,
    clean_doi,
    cast(year AS INT64) AS year,
    last_names,
    NULL AS references, --noqa: L029
    "arxiv" AS dataset
  FROM {{ staging_dataset }}.arxiv_metadata
  UNION ALL
  SELECT
    cast(id AS STRING) AS id,
    title,
    abstract,
    NULL AS clean_doi,
    cast(year AS INT64) AS year,
    last_names,
    NULL AS references, --noqa: L029
    "pwc" AS dataset
  FROM {{ staging_dataset }}.papers_with_code_metadata
  UNION ALL
  SELECT
    id,
    title,
    abstract,
    clean_doi,
    year,
    last_names,
    references,
    "openalex" AS dataset
  FROM {{ staging_dataset }}.openalex_metadata
  UNION ALL
  SELECT
    id,
    title,
    abstract,
    clean_doi,
    year,
    last_names,
    references,
    "s2" AS dataset
  FROM {{ staging_dataset }}.s2_metadata
  UNION ALL
  SELECT
    id,
    title,
    abstract,
    clean_doi,
    year,
    last_names,
    references,
    "lens" AS dataset
  FROM {{ staging_dataset }}.lens_metadata
),

-- add merged id refs
mapped_references AS (
  SELECT
    id,
    array_to_string(array_agg(DISTINCT merged_id ORDER BY merged_id), ",") AS references --noqa: L029
  FROM
    meta
  CROSS JOIN unnest(split(references, ",")) AS orig_id_ref
  INNER JOIN
    {{ production_dataset }}.sources
    ON orig_id_ref = orig_id
  GROUP BY id
)

SELECT
  id,
  title,
  abstract,
  clean_doi,
  year,
  last_names,
  references,
  dataset
FROM
  meta
UNION ALL
SELECT
  id,
  title,
  abstract,
  clean_doi,
  year,
  last_names,
  mapped_references.references,
  dataset
FROM
  meta
INNER JOIN
  mapped_references
  USING (id)
WHERE array_length(split(meta.references, ",")) = array_length(split(mapped_references.references, ","))
