-- create "merged metadata" by simply taking the max value of our metadata columns of interest
select
  merged_id,
  max(title) as title,
  max(abstract) as abstract,
  max(clean_doi) as doi,
  max(year) as year,
  split(max(last_names_norm), " ") as normalized_last_names
from {{params.dataset}}.article_links_with_meta group by merged_id