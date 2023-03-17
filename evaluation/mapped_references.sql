-- glue all the metadata together into one table
with meta as (
  select cast(id as string) as id, title, abstract, clean_doi, cast(year as int64) as year, last_names,
    null as references, "arxiv" as dataset
    from staging_gcp_cset_links.arxiv_metadata
  UNION ALL
  select cast(id as string) as id, title, abstract, clean_doi, cast(year as int64) as year, last_names,
    references, "wos" as dataset
    from staging_gcp_cset_links.wos_metadata
  UNION ALL
  select cast(id as string) as id, title, abstract, clean_doi, cast(year as int64) as year, last_names,
    references, "ds" as dataset
    from staging_gcp_cset_links.ds_metadata
  UNION ALL
  select cast(id as string) as id, title, abstract, clean_doi, cast(year as int64) as year, last_names,
    references, "mag" as dataset
  from staging_gcp_cset_links.mag_metadata
  UNION ALL
  select cast(id as string) as id, title, abstract, clean_doi, cast(year as int64) as year, last_names,
    null as references, "cnki" as dataset
  from staging_gcp_cset_links.cnki_metadata
  UNION ALL
  select cast(id as string) as id, title, abstract, null as clean_doi, cast(year as int64) as year, last_names,
    null as references, "pwc" as dataset
  from staging_gcp_cset_links.papers_with_code_metadata
  UNION ALL
  select id, title, abstract, clean_doi, year, last_names,
    references, "openalex" as dataset
  from staging_gcp_cset_links.openalex_metadata
),
-- add merged id refs
mapped_references as (
  select
    id,
    array_to_string(array_agg(distinct merged_id order by merged_id), ",") as references
  from
    meta
  cross join unnest(split(references, ",")) as orig_id_ref
    inner join
    gcp_cset_links_v2.article_links
  on orig_id_ref = orig_id
  group by id
)

select
  id,
  title,
  abstract,
  clean_doi,
  year,
  last_names,
  mapped_references.references,
  dataset
from
  meta
inner join
  mapped_references
using(id)
where array_length(split(meta.references, ",")) = array_length(split(mapped_references.references, ","))
