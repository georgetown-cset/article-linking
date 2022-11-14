select 
  merged_id,
  data1.orig_id as orig_id1,
  data2.orig_id as orig_id2,
  data1.metadata as metadata1,
  data2.metadata as metadata2
from article_links_v3_eval.positive_match_no_simhash as data1
inner join 
article_links_v3_eval.positive_match_no_simhash as data2
using(merged_id)
where data1.orig_id > data2.orig_id -- avoid having annotators annotate twice