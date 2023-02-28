-- get s2 article ids (used in validation)
SELECT
  DISTINCT(CAST(corpusid AS string)) as id
FROM
  semantic_scholar.papers
CROSS JOIN unnest(publicationtypes) as publication_type
WHERE NOT (publication_type IN ("Dataset", "LettersAndComments", "ClinicalTrial", "Review"))