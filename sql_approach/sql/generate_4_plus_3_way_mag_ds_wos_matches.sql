select distinct * from
(select * from {DATASET}.first_4_plus_3_way_mag_ds_matches
union all
(select
  arxiv_id,
  wos_id,
  null as ds_id,
  mag_id
from {DATASET}.arxiv_wos_mag_first
where (arxiv_id not in (select arxiv_id from {DATASET}.first_4_plus_3_way_mag_ds_matches)) and
  (wos_id not in (select wos_id from {DATASET}.first_4_plus_3_way_mag_ds_matches)) and
  (mag_id not in (select mag_id from {DATASET}.first_4_plus_3_way_mag_ds_matches))))
