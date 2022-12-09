-- aggregate pwc metadata
select
  paper_url as id,
  title,
  abstract,
  extract(year from date) as year,
  -- these are actually full names, but they will be turned into last names by the cleaning script
  authors as last_names,
  references
from papers_with_code.papers_with_abstracts
left join
{{ staging_dataset }}.orig_id_references
on paper_url = orig_id_references.orig_id