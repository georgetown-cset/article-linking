-- get author last names
SELECT
  id,
  array_agg(last_name IGNORE NULLS) AS last_names
FROM gcp_cset_clarivate.wos_summary_names_latest
WHERE role = "author"
GROUP BY id
