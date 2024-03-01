-- get openalex combined metadata used in matching
WITH
author_names AS (
  SELECT
    id,
    ARRAY_AGG(authorship.author.display_name) AS last_names
  FROM
    openalex.works
  CROSS JOIN
    UNNEST(authorships) AS authorship
  WHERE
    authorship.author.display_name IS NOT NULL
  GROUP BY
    id )

SELECT
  id,
  title,
  abstract,
  REPLACE(LOWER(doi), "https://doi.org/", "") AS clean_doi,
  publication_year AS year,
  -- full names, not last names, but the cleaning script will turn them into last names
  last_names,
  ARRAY_TO_STRING(ARRAY(
    SELECT r
    FROM
      UNNEST(referenced_works) AS r
    ORDER BY
      r), ",") AS references --noqa: L029
FROM
  openalex.works
LEFT JOIN
  author_names
  USING
    (id)
WHERE
  (type IS NULL)
  OR NOT (type IN ("dataset",
      "peer-review",
      "grant"))
