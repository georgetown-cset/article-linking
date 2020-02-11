select distinct * from
(select * from {DATASET}.first_4_plus_3_way_mag_matches
union all
(select
  arxiv_id,
  null as wos_id,
  ds_id,
  mag_id
from {DATASET}.arxiv_ds_mag_first
where (arxiv_id not in (select arxiv_id from {DATASET}.first_4_plus_3_way_mag_matches)) and
  (mag_id not in (select mag_id from {DATASET}.first_4_plus_3_way_mag_matches)) and
  (ds_id not in (select ds_id from {DATASET}.first_4_plus_3_way_mag_matches))))
