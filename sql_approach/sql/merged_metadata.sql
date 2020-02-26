select
  id,
  max(title) as title,
  max(abstract) as abstract,
  max(clean_doi) as doi,
  max(year) as year,
  string_split(max(last_names_norm), " ") as normalized_last_names
from {DATASET}.article_links_with_meta group by id