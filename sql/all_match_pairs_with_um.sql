-- add "self matches" for the articles that didn't match anything (this can happen if the article has a lot of null
-- fields) to the rest of the article match pairs
WITH
  oa_matches AS (
  SELECT
    id AS id1,
    ids.mag AS id2
  FROM
    openalex.works
  WHERE
    (ids.mag IS NOT NULL) AND ((type IS NULL)
        OR NOT (type IN ("dataset", "peer-review", "grant")))
  UNION ALL
  SELECT
    ids.mag AS id1,
    id AS id2
  FROM
    openalex.works
  WHERE
    (ids.mag IS NOT NULL) AND ((type IS NULL)
        OR NOT (type IN ("dataset", "peer-review", "grant")))
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
        {{staging_dataset}}.metadata_self_triple_match)
      AND id NOT IN (
      SELECT
        id1
      FROM
        oa_matches))
  UNION ALL (
    SELECT
      all1_id AS id1,
      all2_id AS id2
    FROM
      {{staging_dataset}}.metadata_self_triple_match)
  UNION ALL
  SELECT
    id1,
    id2
  FROM
    oa_matches )
SELECT
  DISTINCT id1,
  id2
FROM
  pairs