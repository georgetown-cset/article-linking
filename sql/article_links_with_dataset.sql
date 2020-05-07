-- add orig_id dataset to the article_links table
select distinct
  a.merged_id,
  a.orig_id,
  b.dataset
from {{params.dataset}}.article_links a
left join
{{params.dataset}}.union_metadata b
on a.orig_id = b.id