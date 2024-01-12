-- get arxiv author last names
SELECT
  id,
  ARRAY(SELECT keyname FROM UNNEST(authors.author)) AS last_names
FROM gcp_cset_arxiv_metadata.arxiv_metadata_latest
