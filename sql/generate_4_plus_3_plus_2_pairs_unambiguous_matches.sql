(select *
from {DATASET}.all_unambiguous_pairs
where ((arxiv_id is not null) and
  (arxiv_id not in (select arxiv_id from {DATASET}.all_4_plus_3_way_unambiguous_filt where arxiv_id is not null))) and
  ((wos_id is not null) and
  (wos_id not in (select wos_id from {DATASET}.all_4_plus_3_way_unambiguous_filt where wos_id is not null))))
union all
(select *
from {DATASET}.all_unambiguous_pairs
where ((ds_id is not null) and
  (ds_id not in (select ds_id from {DATASET}.all_4_plus_3_way_unambiguous_filt where ds_id is not null))) and
  ((wos_id is not null) and
  (wos_id not in (select wos_id from {DATASET}.all_4_plus_3_way_unambiguous_filt where wos_id is not null))))
union all
(select *
from {DATASET}.all_unambiguous_pairs
where ((arxiv_id is not null) and
  (arxiv_id not in (select arxiv_id from {DATASET}.all_4_plus_3_way_unambiguous_filt where arxiv_id is not null))) and
  ((mag_id is not null) and
  (mag_id not in (select mag_id from {DATASET}.all_4_plus_3_way_unambiguous_filt where mag_id is not null))))
union all
(select *
from {DATASET}.all_unambiguous_pairs
where ((ds_id is not null) and
  (ds_id not in (select ds_id from {DATASET}.all_4_plus_3_way_unambiguous_filt where ds_id is not null))) and
  ((mag_id is not null) and
  (mag_id not in (select mag_id from {DATASET}.all_4_plus_3_way_unambiguous_filt where mag_id is not null))))
union all
(select *
from {DATASET}.all_unambiguous_pairs
where ((ds_id is not null) and
  (ds_id not in (select ds_id from {DATASET}.all_4_plus_3_way_unambiguous_filt where ds_id is not null))) and
  ((arxiv_id is not null) and
  (arxiv_id not in (select arxiv_id from {DATASET}.all_4_plus_3_way_unambiguous_filt where arxiv_id is not null))))
union all
(select *
from {DATASET}.all_unambiguous_pairs
where ((wos_id is not null) and
  (wos_id not in (select wos_id from {DATASET}.all_4_plus_3_way_unambiguous_filt where wos_id is not null))) and
  ((mag_id is not null) and
  (mag_id not in (select mag_id from {DATASET}.all_4_plus_3_way_unambiguous_filt where mag_id is not null))))
