SELECT DISTINCT merged_id
FROM
  literature.sources
WHERE
  orig_id IN (SELECT id1 FROM staging_literature.unlink)
