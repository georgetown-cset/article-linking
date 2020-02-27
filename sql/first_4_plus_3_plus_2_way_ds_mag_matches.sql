select distinct * from
(select * from {DATASET}.first_4_plus_3_way_mag_ds_wos_arxiv_matches
union all
(select *
from {DATASET}.all_pairs
where ((ds_id is not null) and
  (ds_id not in (select ds_id from {DATASET}.first_4_plus_3_way_mag_ds_wos_arxiv_matches where ds_id is not null))) and
  ((mag_id is not null) and
  (mag_id not in (select mag_id from {DATASET}.first_4_plus_3_way_mag_ds_wos_arxiv_matches where mag_id is not null)))))