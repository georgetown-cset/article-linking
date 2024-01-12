-- add "self matches" for the articles that didn't match anything (this can happen if the article has a lot of null
-- fields) to the rest of the article match pairs
WITH lens_matches AS (
  (SELECT
      lens_id AS id1,
      CONCAT("https://openalex.org/", id.value) AS id2
    FROM
      lens.scholarly
    CROSS JOIN
      UNNEST(external_ids) AS id
    WHERE
      (id.type = "openalex")
      AND lens_id IN (SELECT id FROM {{ staging_dataset }}.lens_ids)
  )
  UNION ALL
  (
    SELECT
      CONCAT("https://openalex.org/", id.value) AS id1,
      lens_id AS id2
    FROM
      lens.scholarly
    CROSS JOIN
      UNNEST(external_ids) AS id
    WHERE
      (id.type = "openalex")
      AND lens_id IN (SELECT id FROM {{ staging_dataset }}.lens_ids)
  )
  UNION ALL
  (
    SELECT
      lens_id AS id1,
      alias_lens_id AS id2
    FROM lens.scholarly
    CROSS JOIN UNNEST(alias_lens_ids) AS alias_lens_id
  )
  UNION ALL
  (
    SELECT
      alias_lens_id AS id1,
      lens_id AS id2
    FROM lens.scholarly
    CROSS JOIN UNNEST(alias_lens_ids) AS alias_lens_id
  )
),

raw_oa_arxiv_matches AS (
  SELECT
    id AS oa_id,
    REPLACE(REGEXP_REPLACE(open_access.oa_url, r".*(/ftp/arxiv/papers/|/pdf/|/abs/)", ""), ".pdf", "") AS arxiv_id
  FROM
    openalex.works
  WHERE
    (LOWER(open_access.oa_url) LIKE "%/arxiv.org%")
),

oa_arxiv_matches AS (
  SELECT
    oa_id AS id1,
    arxiv_id AS id2
  FROM
    raw_oa_arxiv_matches
  UNION ALL
  SELECT
    arxiv_id AS id1,
    oa_id AS id2
  FROM
    raw_oa_arxiv_matches
),

pairs AS ( (
    SELECT
      id AS id1,
      id AS id2
    FROM
      {{ staging_dataset }}.all_metadata_norm_filt
    WHERE
      id NOT IN (
        SELECT all1_id
        FROM
          {{ staging_dataset }}.metadata_match)
      AND id NOT IN (
        SELECT id1
        FROM
          oa_arxiv_matches)
      AND id NOT IN (
        SELECT id1
        FROM
          lens_matches))
  UNION ALL (
    SELECT
      all1_id AS id1,
      all2_id AS id2
    FROM
      {{ staging_dataset }}.metadata_match)
  UNION ALL
  SELECT
    id1,
    id2
  FROM
    oa_arxiv_matches
  UNION ALL
  SELECT
    id1,
    id2
  FROM
    lens_matches
)

SELECT DISTINCT
  id1,
  id2
FROM
  pairs
