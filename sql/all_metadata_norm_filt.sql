with meaningfully_titled as (
  select title_norm from (select title_norm, count(distinct(id)) as num_ids from staging_gcp_cset_links.all_metadata_norm group by title_norm) where num_ids < 11
),

meaningfully_doied as (
  select clean_doi from (select clean_doi, count(distinct(id)) as num_ids from staging_gcp_cset_links.all_metadata_norm group by clean_doi) where num_ids < 11
),

meaningfully_abstracted as (
  select abstract_norm from (select abstract_norm, count(distinct(id)) as num_ids from staging_gcp_cset_links.all_metadata_norm group by abstract_norm) where num_ids < 11
)

select
  id,
  title,
  abstract,
  case when clean_doi in (select clean_doi from meaningfully_doied) then clean_doi else null end as clean_doi,
  year,
  last_names,
  references,
  dataset,
  case when title_norm in (select title_norm from meaningfully_titled) then title_norm else null end as title_norm,
  case when abstract_norm in (select abstract_norm from meaningfully_abstracted) then abstract_norm else null end as abstract_norm,
  last_names_norm
from staging_gcp_cset_links.all_metadata_norm
