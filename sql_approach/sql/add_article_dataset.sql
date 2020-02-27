select
  a.merged_id,
  a.orig_id,
  b.dataset
from {DATASET}.article_links a
left join
{DATASET}.all_metadata b
on a.orig_id = b.id