-- get openalex article ids (used in validation)
SELECT DISTINCT id
FROM
  openalex.works
WHERE
  (type IS NULL)
  OR NOT (type IN ("dataset",
      "peer-review",
      "grant",
      "supplementary-materials"))
