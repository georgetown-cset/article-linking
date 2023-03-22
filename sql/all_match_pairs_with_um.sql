-- add "self matches" for the articles that didn't match anything (this can happen if the article has a lot of null
-- fields) to the rest of the article match pairs
WITH oa_matches AS (
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
  s2_matches AS (
    SELECT
      CAST(corpusid AS string) as id1,
      CAST(externalids.MAG AS string) as id2
    FROM
      semantic_scholar.papers
    CROSS JOIN unnest(publicationtypes) as publication_type
    WHERE
      (externalids.MAG is not null) and NOT
        (publication_type IN ("Dataset", "Editorial", "LettersAndComments", "News", "Review"))
    UNION ALL
    SELECT
      CAST(externalids.MAG AS string) as id1,
      CAST(corpusid AS string) as id2
    FROM
      semantic_scholar.papers
    CROSS JOIN unnest(publicationtypes) as publication_type
    WHERE
      (externalids.MAG is not null) and NOT
        (publication_type IN ("Dataset", "Editorial", "LettersAndComments", "News", "Review"))
  ),
  ds_arxiv_matches AS (
    SELECT
      id as id1,
      replace(arxiv_id, "arXiv:", "") as id2
    FROM
      gcp_cset_digital_science.dimensions_publications_latest
    WHERE
      (arxiv_id is not null)
    UNION ALL
    SELECT
      replace(arxiv_id, "arXiv:", "") as id1,
      id as id2
    FROM
      gcp_cset_digital_science.dimensions_publications_latest
    WHERE
      (arxiv_id is not null)
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
      {{staging_dataset}}.all_metadata_norm
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
        oa_matches) AND id NOT IN (
      SELECT
        id1
      FROM
        s2_matches
      ) AND id NOT IN (
      SELECT
        id1
      FROM
        ds_arxiv_matches)
      AND id NOT IN (
      SELECT
        id1
      FROM
        oa_arxiv_matches))
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
      oa_matches
    UNION ALL
    SELECT
      id1,
      id2
    FROM
      s2_matches 
    UNION ALL
    SELECT
      id1,
      id2
    FROM
      ds_arxiv_matches
    UNION ALL
    SELECT
      id1,
      id2
    FROM
      oa_arxiv_matches
  )
SELECT
  DISTINCT id1,
  id2
FROM
  pairs
