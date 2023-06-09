-- get lens article ids (used in validation)
SELECT
  DISTINCT(lens_id) as id
FROM
  lens.scholarly
WHERE (publication_type IS NULL) OR (NOT (publication_type IN ("dataset", "editorial", "letter", "news", "review")))
