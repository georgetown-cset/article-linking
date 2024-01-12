-- add orig_id dataset to the sources table
SELECT DISTINCT
  a.merged_id,
  a.orig_id,
  b.dataset
FROM {{ staging_dataset }}.id_mapping AS a
INNER JOIN
  {{ staging_dataset }}.union_metadata AS b
  ON a.orig_id = b.id
