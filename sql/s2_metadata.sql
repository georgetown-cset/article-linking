-- get s2 combined metadata used in matching
WITH paper_authors AS (
  SELECT
    corpusid,
    -- full names, not last names, but the cleaning script will turn them into last names
    ARRAY_AGG(author.name) AS last_names
  FROM
    semantic_scholar.papers
  CROSS JOIN UNNEST(authors) AS author
  GROUP BY corpusid
),

paper_references AS (
  SELECT
    citingcorpusid AS corpusid,
    ARRAY_TO_STRING(ARRAY_AGG(CAST(citedcorpusid AS STRING) ORDER BY citedcorpusid), ",") AS references --noqa: L029
  FROM
    semantic_scholar.citations
  GROUP BY
    corpusid
)

SELECT
  CAST(corpusid AS STRING) AS id,
  title,
  abstract,
  REPLACE(LOWER(externalids.DOI), "https://doi.org/", "") AS clean_doi,
  EXTRACT(YEAR FROM publicationdate) AS year,
  last_names,
  references
FROM
  semantic_scholar.papers
INNER JOIN
  {{ staging_dataset }}.s2_ids
  ON CAST(corpusid AS STRING) = id
LEFT JOIN
  semantic_scholar.abstracts
  USING (corpusid)
LEFT JOIN
  paper_authors
  USING (corpusid)
LEFT JOIN
  paper_references
  USING (corpusid)
