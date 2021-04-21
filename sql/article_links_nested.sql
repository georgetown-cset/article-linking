select
  merged_id,
  array_agg(orig_id) as orig_ids
from
  {{ params.dataset }}.article_links
group by merged_id