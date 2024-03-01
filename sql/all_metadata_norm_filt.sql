WITH meaningfully_titled AS (
  SELECT title_norm
  FROM (SELECT
    title_norm,
    count(distinct(id)) AS num_ids
    FROM {{ staging_dataset }}.all_metadata_norm GROUP BY title_norm) WHERE num_ids < 11
),

meaningfully_doied AS (
  SELECT clean_doi
  FROM (SELECT
    clean_doi,
    count(distinct(id)) AS num_ids
    FROM {{ staging_dataset }}.all_metadata_norm GROUP BY clean_doi) WHERE num_ids < 11
),

meaningfully_abstracted AS (
  SELECT abstract_norm
  FROM (SELECT
    abstract_norm,
    count(distinct(id)) AS num_ids
    FROM {{ staging_dataset }}.all_metadata_norm GROUP BY abstract_norm) WHERE num_ids < 11
)

SELECT
  id,
  title,
  abstract,
  CASE WHEN clean_doi IN (SELECT clean_doi FROM meaningfully_doied) THEN clean_doi END AS clean_doi,
  year,
  last_names,
  references,
  dataset,
  CASE WHEN title_norm IN (SELECT title_norm FROM meaningfully_titled) THEN title_norm END AS title_norm,
  CASE
    WHEN abstract_norm IN (SELECT abstract_norm FROM meaningfully_abstracted) THEN abstract_norm
  END AS abstract_norm,
  last_names_norm
FROM {{ staging_dataset }}.all_metadata_norm
