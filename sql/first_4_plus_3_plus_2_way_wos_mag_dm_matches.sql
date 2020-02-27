select distinct * from
(select * from {DATASET}.first_4_plus_3_plus_2_way_ds_mag_matches
union all
(select *
from {DATASET}.all_pairs
where ((wos_id is not null) and
  (wos_id not in (select wos_id from {DATASET}.first_4_plus_3_plus_2_way_ds_mag_matches where wos_id is not null))) and
  ((mag_id is not null) and
  (mag_id not in (select mag_id from {DATASET}.first_4_plus_3_plus_2_way_ds_mag_matches where mag_id is not null)))))
