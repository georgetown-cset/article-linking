select distinct * from
(select * from {DATASET}.all_4_way
union all
--- get the three-way matches that couldn't be fully linked
(select
  arxiv_id,
  wos_id,
  ds_id,
  null as mag_id
from {DATASET}.arxiv_wos_ds_triples
where (arxiv_id not in (select arxiv_id from {DATASET}.all_4_way)) and
  (wos_id not in (select wos_id from {DATASET}.all_4_way)) and
  (ds_id not in (select ds_id from {DATASET}.all_4_way)))
union all
(select
  arxiv_id,
  wos_id,
  null as ds_id,
  mag_id
from {DATASET}.arxiv_wos_mag_triples
where (arxiv_id not in (select arxiv_id from {DATASET}.all_4_way)) and
  (wos_id not in (select wos_id from {DATASET}.all_4_way)) and
  (mag_id not in (select mag_id from {DATASET}.all_4_way)))
union all
(select
  arxiv_id,
  null as wos_id,
  ds_id,
  mag_id
from {DATASET}.arxiv_ds_mag_triples
where (arxiv_id not in (select arxiv_id from {DATASET}.all_4_way)) and
  (mag_id not in (select mag_id from {DATASET}.all_4_way)) and
  (ds_id not in (select ds_id from {DATASET}.all_4_way)))
union all
(select
  null as arxiv_id,
  wos_id,
  ds_id,
  mag_id
from {DATASET}.wos_ds_mag_triples
where (wos_id not in (select wos_id from {DATASET}.all_4_way)) and
  (mag_id not in (select mag_id from {DATASET}.all_4_way)) and
  (ds_id not in (select ds_id from {DATASET}.all_4_way))))