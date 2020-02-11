select distinct * from
(select * from {DATASET}.first_4_plus_3_plus_2_way_arxiv_wos_ad_am_wd_wm_dm_matches
union all
--- finally, add in the ids we couldn't match to anything
(select
  id as arxiv_id,
  null as wos_id,
  null as ds_id,
  null as mag_id
from {DATASET}.arxiv_metadata
where (id not in (select arxiv_id from {DATASET}.first_4_plus_3_plus_2_way_arxiv_wos_ad_am_wd_wm_dm_matches where arxiv_id is not null)))
union all
(select
  null as arxiv_id,
  id as wos_id,
  null as ds_id,
  null as mag_id
from {DATASET}.wos_metadata
where (id not in (select wos_id from {DATASET}.first_4_plus_3_plus_2_way_arxiv_wos_ad_am_wd_wm_dm_matches where wos_id is not null)))
union all
(select
  null as arxiv_id,
  null as wos_id,
  id as ds_id,
  null as mag_id
from {DATASET}.ds_metadata
where (id not in (select ds_id from {DATASET}.first_4_plus_3_plus_2_way_arxiv_wos_ad_am_wd_wm_dm_matches where ds_id is not null)))
union all
(select
  null as arxiv_id,
  null as wos_id,
  null as ds_id,
  id as mag_id
from {DATASET}.mag_metadata
where (id not in (select mag_id from {DATASET}.first_4_plus_3_plus_2_way_arxiv_wos_ad_am_wd_wm_dm_matches where mag_id is not null))))
