select distinct * from
(select * from {DATASET}.first_4_plus_3_plus_2_way_arxiv_mag_wd_wm_dm_matches
union all
(select *
from {DATASET}.all_pairs
where ((arxiv_id is not null) and
  (arxiv_id not in (select arxiv_id from {DATASET}.first_4_plus_3_plus_2_way_arxiv_mag_wd_wm_dm_matches where arxiv_id is not null))) and
  ((ds_id is not null) and
  (ds_id not in (select ds_id from {DATASET}.first_4_plus_3_plus_2_way_arxiv_mag_wd_wm_dm_matches where ds_id is not null)))))