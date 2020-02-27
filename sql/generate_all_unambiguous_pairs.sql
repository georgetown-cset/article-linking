select distinct * from ((select
  arxiv_id,
  null as wos_id,
  null as ds_id,
  mag_id
from {DATASET}.arxiv_mag_unambiguous)
union all
(select
  arxiv_id,
  null as wos_id,
  ds_id,
  null as mag_id
from {DATASET}.arxiv_ds_unambiguous)
union all
(select
  arxiv_id,
  wos_id,
  null as ds_id,
  null as mag_id
from {DATASET}.arxiv_wos_unambiguous)
union all
(select
  null as arxiv_id,
  wos_id,
  ds_id,
  null as mag_id
from {DATASET}.wos_ds_unambiguous)
union all
(select
  null as arxiv_id,
  wos_id,
  null as ds_id,
  mag_id
from {DATASET}.wos_mag_unambiguous)
union all
(select
  null as arxiv_id,
  null as wos_id,
  ds_id,
  mag_id
from {DATASET}.ds_mag_unambiguous))