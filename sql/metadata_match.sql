-- get combined set of metadata matches
SELECT DISTINCT
  all1_id,
  all2_id
FROM (
  {{ params.tables }}
)
