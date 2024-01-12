-- get s2 article ids (used in validation)
SELECT DISTINCT CAST(corpusid AS STRING) AS id
FROM
  semantic_scholar.papers
LEFT JOIN UNNEST(publicationtypes) AS publication_type
WHERE
  (
    publication_type IS NULL
  ) OR (NOT(publication_type IN ("Dataset", "Editorial", "LettersAndComments", "News", "Review")))
