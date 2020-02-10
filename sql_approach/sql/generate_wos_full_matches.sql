select distinct * from ((select
  b.arxiv_id,
  a.wos_id,
  b.ds_id,
  b.mag_id
from {DATASET}.all_unambiguous_pairs a
inner join
{DATASET}.arxiv_ds_mag_unambiguous b
on (a.arxiv_id = b.arxiv_id) and (a.wos_id is not null))
union all
(select
  b.arxiv_id,
  a.wos_id,
  b.ds_id,
  b.mag_id
from {DATASET}.all_unambiguous_pairs a
inner join
{DATASET}.arxiv_ds_mag_unambiguous b
on (a.ds_id = b.ds_id) and (a.wos_id is not null))
union all
(select
  b.arxiv_id,
  a.wos_id,
  b.ds_id,
  b.mag_id
from {DATASET}.all_unambiguous_pairs a
inner join
{DATASET}.arxiv_ds_mag_unambiguous b
on (a.mag_id = b.mag_id) and (a.wos_id is not null)))