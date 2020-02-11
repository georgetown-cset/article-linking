select distinct * from
(select * from {DATASET}.first_4_plus_3_way_mag_ds_wos_matches
union all
(select
  arxiv_id,
  wos_id,
  ds_id,
  null as mag_id
from {DATASET}.arxiv_wos_ds_first
where (arxiv_id not in (select arxiv_id from {DATASET}.first_4_plus_3_way_mag_ds_wos_matches)) and
  (wos_id not in (select wos_id from {DATASET}.first_4_plus_3_way_mag_ds_wos_matches)) and
  (ds_id not in (select ds_id from {DATASET}.first_4_plus_3_way_mag_ds_wos_matches))))
