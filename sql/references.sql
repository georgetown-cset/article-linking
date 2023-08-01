-- Create a table giving each publication's references, identified by their merged_ids.
-- The all_metadata_with_cld2_lid table contains references identified by vendor IDs for each orig_id.
-- For each merged_id, we take all its orig_ids' references, and look up the merged_ids of the references.
-- We exclude references that appear outside our merged corpus.
SELECT
  DISTINCT links1.merged_id AS merged_id,
  links2.merged_id AS ref_id
FROM (
  SELECT
    id,
    reference
  FROM
    {{ staging_dataset }}.all_metadata_with_cld2_lid
  CROSS JOIN
    UNNEST(SPLIT(references, ",")) AS reference
  WHERE
    reference IN (
    SELECT
      orig_id
    FROM
      {{ staging_dataset }}.sources )) AS references
LEFT JOIN
  {{ staging_dataset }}.article_links AS links1
ON
references.id = links1.orig_id
LEFT JOIN
  {{ staging_dataset }}.article_links AS links2
ON
references.reference = links2.orig_id
WHERE
  (links1.merged_id IS NOT NULL)
  AND (links2.merged_id IS NOT NULL)
