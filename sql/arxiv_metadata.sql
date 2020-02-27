select
  p.id,
  p.title,
  p.abstract,
  lower(p.doi) as clean_doi,
  extract(year from p.created) as year,
  a.last_names,
  null as references -- arxiv doesn't have references
from gcp_cset_arxiv_metadata.arxiv_metadata_latest p
left join
{DATASET}.arxiv_authors a
on a.id = p.id
