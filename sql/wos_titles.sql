-- get wos titles (note that there may be more than one per article in different languages)
SELECT
  id,
  title_id AS title_id,
  title AS title
FROM gcp_cset_clarivate.wos_titles_latest
WHERE title_type = "item"
