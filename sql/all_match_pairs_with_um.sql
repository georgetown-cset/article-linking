-- add "self matches" for the articles that didn't match anything (this can happen if the article has a lot of null
-- fields) to the rest of the article match pairs
WITH lens_matches AS (
    (SELECT
      lens_id as id1,
      CONCAT("https://openalex.org/", id.value) as id2
    FROM
      lens.scholarly
    CROSS JOIN
      UNNEST(external_ids) as id
    WHERE
      (id.type = "openalex")
      AND lens_id in (select id from {{ staging_dataset }}.lens_ids)
    )
    UNION ALL
    (
    SELECT
      CONCAT("https://openalex.org/", id.value) as id1,
      lens_id as id2
    FROM
      lens.scholarly
    CROSS JOIN
      UNNEST(external_ids) as id
    WHERE
      (id.type = "openalex")
      AND lens_id in (select id from {{ staging_dataset }}.lens_ids)
    )
    (
    SELECT
      lens_id as id1,
      alias_lens_id as id2
    FROM `gcp-cset-projects.lens.scholarly`
    CROSS JOIN UNNEST(alias_lens_ids) as alias_lens_id
    )
    UNION ALL
    (
    SELECT
      alias_lens_id as id1,
      lens_id as id2,
    FROM `gcp-cset-projects.lens.scholarly`
    CROSS JOIN UNNEST(alias_lens_ids) as alias_lens_id
    )
  ),
  raw_oa_arxiv_matches AS (
    SELECT
      id as oa_id,
      replace(regexp_replace(open_access.oa_url, r".*(/ftp/arxiv/papers/|/pdf/|/abs/)", ""), ".pdf", "") as arxiv_id
    FROM
      openalex.works
    WHERE
      (lower(open_access.oa_url) like "%/arxiv.org%")
  ),
  oa_arxiv_matches AS (
    SELECT
      oa_id as id1,
      arxiv_id as id2
    FROM
      raw_oa_arxiv_matches
    UNION ALL
    SELECT
      arxiv_id as id1,
      oa_id as id2
    FROM
      raw_oa_arxiv_matches
  ),
  pairs AS ( (
    SELECT
      id AS id1,
      id AS id2
    FROM
      {{staging_dataset}}.all_metadata_norm_filt
    WHERE
      id NOT IN (
      SELECT
        all1_id
      FROM
        {{staging_dataset}}.metadata_match)
      AND id NOT IN (
      SELECT
        id1
      FROM
        oa_arxiv_matches)
      AND id NOT IN (
      SELECT
        id1
      FROM
        lens_matches))
    UNION ALL (
      SELECT
        all1_id AS id1,
        all2_id AS id2
      FROM
        {{staging_dataset}}.metadata_match)
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
SELECT
  DISTINCT id1,
  id2
FROM
  pairs
