select distinct * from ((select
  a.arxiv_id,
  b.wos_id,
  b.ds_id,
  b.mag_id
from {DATASET}.all_first_pairs a
inner join
{DATASET}.wos_ds_mag_first b
on (a.wos_id = b.wos_id) and (a.arxiv_id is not null))
union all
(select
  a.arxiv_id,
  b.wos_id,
  b.ds_id,
  b.mag_id
from {DATASET}.all_first_pairs a
inner join
{DATASET}.wos_ds_mag_first b
on (a.ds_id = b.ds_id) and (a.arxiv_id is not null))
union all
(select
  a.arxiv_id,
  b.wos_id,
  b.ds_id,
  b.mag_id
from {DATASET}.all_first_pairs a
inner join
{DATASET}.wos_ds_mag_first b
on (a.mag_id = b.mag_id) and (a.arxiv_id is not null)))
