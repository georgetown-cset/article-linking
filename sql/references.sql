-- Create a table giving each publication's references, identified by their merged_ids.
-- The all_metadata_with_cld2_lid table contains references identified by vendor IDs for each orig_id.
-- For each merged_id, we take all its orig_ids' references, and look up the merged_ids of the references.
-- We exclude references that appear outside our merged corpus.
WITH references AS (
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
      {{ staging_dataset }}.article_links )
)
SELECT
  DISTINCT referencing_papers.merged_id AS merged_id,
  referenced_papers.merged_id AS ref_id
FROM references
LEFT JOIN
  {{ staging_dataset }}.article_links AS referencing_papers
ON
references.id = referencing_papers.orig_id
LEFT JOIN
  {{ staging_dataset }}.article_links AS referenced_papers
ON
references.reference = referenced_papers.orig_id
WHERE
  (referencing_papers.merged_id IS NOT NULL)
  AND (referenced_papers.merged_id IS NOT NULL)
