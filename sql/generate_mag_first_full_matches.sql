select distinct * from ((select
  b.arxiv_id,
  b.wos_id,
  b.ds_id,
  a.mag_id
from {DATASET}.all_first_pairs a
inner join
{DATASET}.arxiv_wos_ds_first b
on (a.arxiv_id = b.arxiv_id) and (a.mag_id is not null))
union all
(select
  b.arxiv_id,
  b.wos_id,
  b.ds_id,
  a.mag_id
from {DATASET}.all_first_pairs a
inner join
{DATASET}.arxiv_wos_ds_first b
on (a.wos_id = b.wos_id) and (a.mag_id is not null))
union all
(select
  b.arxiv_id,
  b.wos_id,
  b.ds_id,
  a.mag_id
from {DATASET}.all_first_pairs a
inner join
{DATASET}.arxiv_wos_ds_first b
on (a.ds_id = b.ds_id) and (a.mag_id is not null)))
