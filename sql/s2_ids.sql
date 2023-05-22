-- get s2 article ids (used in validation)
SELECT
  DISTINCT(CAST(corpusid AS string)) as id
FROM
  semantic_scholar.papers
CROSS JOIN unnest(publicationtypes) as publication_type
WHERE (publication_type IS NULL) OR (NOT (publication_type IN ("Dataset", "Editorial", "LettersAndComments", "News", "Review")))