select distinct * from ((select
  arxiv_id,
  null as wos_id,
  null as ds_id,
  mag_id
from {DATASET}.arxiv_mag_base)
union all
(select
  arxiv_id,
  null as wos_id,
  ds_id,
  null as mag_id
from {DATASET}.arxiv_ds_base)
union all
(select
  arxiv_id,
  wos_id,
  null as ds_id,
  null as mag_id
from {DATASET}.arxiv_wos_base)
union all
(select
  null as arxiv_id,
  wos_id,
  ds_id,
  null as mag_id
from {DATASET}.wos_ds_base)
union all
(select
  null as arxiv_id,
  wos_id,
  null as ds_id,
  mag_id
from {DATASET}.wos_mag_base)
union all
(select
  null as arxiv_id,
  null as wos_id,
  ds_id,
  mag_id
from {DATASET}.ds_mag_base))