select distinct * from ((select
  b.arxiv_id,
  b.wos_id,
  a.ds_id,
  b.mag_id
from {DATASET}.all_first_pairs a
inner join
{DATASET}.arxiv_wos_mag_first b
on (a.arxiv_id = b.arxiv_id) and (a.ds_id is not null))
union all
(select
  b.arxiv_id,
  b.wos_id,
  a.ds_id,
  b.mag_id
from {DATASET}.all_first_pairs a
inner join
{DATASET}.arxiv_wos_mag_first b
on (a.wos_id = b.wos_id) and (a.ds_id is not null))
union all
(select
  b.arxiv_id,
  b.wos_id,
  a.ds_id,
  b.mag_id
from {DATASET}.all_first_pairs a
inner join
{DATASET}.arxiv_wos_mag_first b
on (a.mag_id = b.mag_id) and (a.ds_id is not null)))
