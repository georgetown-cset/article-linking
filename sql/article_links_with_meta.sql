-- add all versions of match set metadata to the merged ids
select
  a.merged_id,
  b.title,
  b.abstract,
  b.year,
  b.clean_doi,
  b.last_names_norm
from {{params.dataset}}.article_links a
left join
{{params.dataset}}.all_metadata_norm b
on a.orig_id = b.id