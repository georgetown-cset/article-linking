-- aggregate pwc metadata
select
  paper_url as id,
  title,
  abstract,
  extract(year from date) as year,
  -- these are actually full names, but they will be turned into last names by the cleaning script
  authors as last_names
from papers_with_code.papers_with_abstracts