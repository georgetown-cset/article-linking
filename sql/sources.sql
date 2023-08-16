-- add orig_id dataset to the sources table
select distinct
  a.merged_id,
  a.orig_id,
  b.dataset
from {{staging_dataset}}.sources a
left join
{{staging_dataset}}.union_metadata b
on a.orig_id = b.id
