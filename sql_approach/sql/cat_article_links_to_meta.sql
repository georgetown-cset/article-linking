select
  a.merge_id,
  b.title,
  b.abstract,
  b.year,
  b.clean_doi,
  b.last_names_norm
from {DATASET}.article_links a
left join
{DATASET}.all_metadata_norm b
on a.orig_id = b.id