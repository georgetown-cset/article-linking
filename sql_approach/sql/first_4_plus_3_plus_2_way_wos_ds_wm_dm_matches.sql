select distinct * from
(select * from {DATASET}.first_4_plus_3_plus_2_way_wos_mag_dm_matches
union all
(select *
from {DATASET}.all_pairs
where ((wos_id is not null) and
  (wos_id not in (select wos_id from {DATASET}.first_4_plus_3_plus_2_way_wos_mag_dm_matches where wos_id is not null))) and
  ((ds_id is not null) and
  (ds_id not in (select ds_id from {DATASET}.first_4_plus_3_plus_2_way_wos_mag_dm_matches where ds_id is not null)))))