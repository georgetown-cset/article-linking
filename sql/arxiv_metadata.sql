-- get arxiv metadata used for matching
SELECT
  arxiv_metadata_latest.id,
  arxiv_metadata_latest.title,
  arxiv_metadata_latest.abstract,
  lower(arxiv_metadata_latest.doi) AS clean_doi,
  extract(YEAR FROM arxiv_metadata_latest.created) AS year,
  a.last_names,
  NULL AS references  --noqa: L029
FROM gcp_cset_arxiv_metadata.arxiv_metadata_latest
LEFT JOIN
  {{ staging_dataset }}.arxiv_authors AS a
  ON a.id = arxiv_metadata_latest.id
