select
  orig_id,
  array_to_string(array_agg(ref_id order by ref_id), ",") as references
from {{ production_dataset }}.article_links
inner join
{{ production_dataset }}.mapped_references
on(id = merged_id)
group by orig_id
