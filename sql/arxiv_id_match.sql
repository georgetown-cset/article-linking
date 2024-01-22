-- papers with code.papers_with_abstracts has an arxiv_id column that is often not null; use this to match
-- on arxiv where possible
WITH arxiv_pwc_mapping AS (
  SELECT
    arxiv_metadata_latest.id AS id1,
    papers_with_abstracts.paper_url AS id2
  FROM
    gcp_cset_arxiv_metadata.arxiv_metadata_latest
  INNER JOIN
    papers_with_code.papers_with_abstracts
    ON arxiv_metadata_latest.id = papers_with_abstracts.arxiv_id
)

SELECT
  id1 AS all1_id,
  id2 AS all2_id
FROM arxiv_pwc_mapping
UNION ALL
SELECT
  id2 AS all1_id,
  id1 AS all2_id
FROM arxiv_pwc_mapping
