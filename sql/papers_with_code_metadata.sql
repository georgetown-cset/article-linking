-- aggregate pwc metadata
SELECT
  paper_url AS id,
  title,
  abstract,
  extract(YEAR FROM date) AS year,
  -- these are actually full names, but they will be turned into last names by the cleaning script
  authors AS last_names
FROM papers_with_code.papers_with_abstracts
