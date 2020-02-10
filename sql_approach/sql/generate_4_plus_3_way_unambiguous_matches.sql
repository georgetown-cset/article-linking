select distinct * from
(select * from {DATASET}.all_4_way_unambiguous_filt
union all
--- get the three-way matches that couldn't be fully linked
(select
  arxiv_id,
  wos_id,
  ds_id,
  null as mag_id
from {DATASET}.arxiv_wos_ds
where (arxiv_id not in (select arxiv_id from {DATASET}.all_4_way_unambiguous_filt)) and
  (wos_id not in (select wos_id from {DATASET}.all_4_way_unambiguous_filt)) and
  (ds_id not in (select ds_id from {DATASET}.all_4_way_unambiguous_filt)))
union all
(select
  arxiv_id,
  wos_id,
  null as ds_id,
  mag_id
from {DATASET}.arxiv_wos_mag
where (arxiv_id not in (select arxiv_id from {DATASET}.all_4_way_unambiguous_filt)) and
  (wos_id not in (select wos_id from {DATASET}.all_4_way_unambiguous_filt)) and
  (mag_id not in (select mag_id from {DATASET}.all_4_way_unambiguous_filt)))
union all
(select
  arxiv_id,
  null as wos_id,
  ds_id,
  mag_id
from {DATASET}.arxiv_ds_mag
where (arxiv_id not in (select arxiv_id from {DATASET}.all_4_way_unambiguous_filt)) and
  (mag_id not in (select mag_id from {DATASET}.all_4_way_unambiguous_filt)) and
  (ds_id not in (select ds_id from {DATASET}.all_4_way_unambiguous_filt)))
union all
(select
  null as arxiv_id,
  wos_id,
  ds_id,
  mag_id
from {DATASET}.wos_ds_mag
where (wos_id not in (select wos_id from {DATASET}.all_4_way_unambiguous_filt)) and
  (mag_id not in (select mag_id from {DATASET}.all_4_way_unambiguous_filt)) and
  (ds_id not in (select ds_id from {DATASET}.all_4_way_unambiguous_filt))))