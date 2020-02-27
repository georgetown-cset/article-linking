select * from {DATASET}.all_4_plus_3_way_unambiguous where
  ((arxiv_id is null) or (arxiv_id in
    (select arxiv_id from (select arxiv_id, count(arxiv_id) as num from {DATASET}.all_4_plus_3_way_unambiguous
      where arxiv_id is not null group by arxiv_id) where num = 1))) and
  ((wos_id is null) or (wos_id in
    (select wos_id from (select wos_id, count(wos_id) as num from {DATASET}.all_4_plus_3_way_unambiguous
      where wos_id is not null group by wos_id) where num = 1))) and
  ((ds_id is null) or (ds_id in
    (select ds_id from (select ds_id, count(ds_id) as num from {DATASET}.all_4_plus_3_way_unambiguous
      where ds_id is not null group by ds_id) where num = 1))) and
  ((mag_id is null) or (mag_id in
    (select mag_id from (select mag_id, count(mag_id) as num from {DATASET}.all_4_plus_3_way_unambiguous
      where mag_id is not null group by mag_id) where num = 1)))