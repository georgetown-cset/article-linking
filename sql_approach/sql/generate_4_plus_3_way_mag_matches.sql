select distinct * from
(select * from {DATASET}.all_4_way_first_filt
union all
(select
  null as arxiv_id,
  wos_id,
  ds_id,
  mag_id
from {DATASET}.wos_ds_mag_first
where (wos_id not in (select wos_id from {DATASET}.all_4_way_first_filt)) and
  (mag_id not in (select mag_id from {DATASET}.all_4_way_first_filt)) and
  (ds_id not in (select ds_id from {DATASET}.all_4_way_first_filt))))
