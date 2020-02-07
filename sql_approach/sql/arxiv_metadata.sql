select
  p.id,
  p.title,
  p.abstract,
  extract(year from p.created) as year,
  a.last_name
from gcp_cset_arxiv_metadata.arxiv_metadata_latest p
left join
{DATASET}.arxiv_authors a
on a.id = p.id