-- get arxiv metadata used for matching
select
  p.id,
  p.title,
  p.abstract,
  lower(p.doi) as clean_doi,
  extract(year from p.created) as year,
  a.last_names,
  -- arxiv doesn't have references, so just use the ones from linkage, if they exist
  references
from gcp_cset_arxiv_metadata.arxiv_metadata_latest p
left join
{{ staging_dataset }}.arxiv_authors a
on a.id = p.id
left join
{{ staging_dataset }}.orig_id_references
on a.id = orig_id_references.orig_id
