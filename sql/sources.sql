-- add orig_id dataset to the article_links table
select distinct
  a.merged_id,
  a.orig_id,
  b.dataset
from {{staging_dataset}}.article_links a
left join
{{staging_dataset}}.union_metadata b
on a.orig_id = b.id