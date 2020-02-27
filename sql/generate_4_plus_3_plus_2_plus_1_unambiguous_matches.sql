select distinct * from
(select * from {DATASET}.all_4_plus_3_plus_2
union all
--- finally, add in the ids we couldn't match to anything
(select
  id as arxiv_id,
  null as wos_id,
  null as ds_id,
  null as mag_id
from gcp_cset_links.arxiv_metadata_norm
where (id not in (select arxiv_id from {DATASET}.all_4_plus_3_plus_2 where arxiv_id is not null)))
union all
(select
  null as arxiv_id,
  id as wos_id,
  null as ds_id,
  null as mag_id
from gcp_cset_links.wos_metadata_norm
where (id not in (select wos_id from {DATASET}.all_4_plus_3_plus_2 where wos_id is not null)))
union all
(select
  null as arxiv_id,
  null as wos_id,
  id as ds_id,
  null as mag_id
from gcp_cset_links.ds_metadata_norm
where (id not in (select ds_id from {DATASET}.all_4_plus_3_plus_2 where ds_id is not null)))
union all
(select
  null as arxiv_id,
  null as wos_id,
  null as ds_id,
  id as mag_id
from gcp_cset_links.mag_metadata_norm
where (id not in (select mag_id from {DATASET}.all_4_plus_3_plus_2 where mag_id is not null))))
