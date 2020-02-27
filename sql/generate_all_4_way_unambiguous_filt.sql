select * from {DATASET}.all_4_way_unambiguous where
  (arxiv_id in (select arxiv_id from
    (select arxiv_id, count(arxiv_id) as num from {DATASET}.all_4_way_unambiguous group by arxiv_id) where num = 1)) and
  (wos_id in (select wos_id from
    (select wos_id, count(wos_id) as num from {DATASET}.all_4_way_unambiguous group by wos_id) where num = 1)) and
  (ds_id in (select ds_id from
    (select ds_id, count(ds_id) as num from {DATASET}.all_4_way_unambiguous group by ds_id) where num = 1)) and
  (mag_id in (select mag_id from
    (select mag_id, count(mag_id) as num from {DATASET}.all_4_way_unambiguous group by mag_id) where num = 1))