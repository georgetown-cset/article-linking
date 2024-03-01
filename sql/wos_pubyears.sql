-- get wos publication year
SELECT
  id,
  pubyear AS year
FROM gcp_cset_clarivate.wos_summary_latest
