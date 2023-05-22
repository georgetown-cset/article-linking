-- get s2 combined metadata used in matching
WITH paper_authors AS (
  SELECT
    corpusid,
    -- full names, not last names, but the cleaning script will turn them into last names
    ARRAY_AGG(author.name) as last_names
  FROM
    semantic_scholar.papers
  CROSS JOIN UNNEST(authors) as author
  GROUP BY corpusid
),
paper_references AS (
  SELECT
    citingcorpusid as corpusid,
    ARRAY_TO_STRING(ARRAY_AGG(CAST(citedcorpusid AS string) ORDER BY citedcorpusid), ",") as references
  FROM
    semantic_scholar.citations
  GROUP BY
    corpusid
)

SELECT
  CAST(corpusid AS string) AS id,
  title,
  abstract,
  REPLACE(LOWER(externalids.DOI), "https://doi.org/", "") AS clean_doi,
  EXTRACT(year FROM publicationdate) as year,
  last_names,
  references
FROM
  semantic_scholar.papers
LEFT JOIN
  semantic_scholar.abstracts
USING(corpusid)
LEFT JOIN
  paper_authors
USING(corpusid)
LEFT JOIN
  paper_references
USING(corpusid)