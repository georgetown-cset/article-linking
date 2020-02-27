select
  a.id,
  a.year,
  a.title_norm,
  a.abstract_trunc_norm_len_filt,
  a.last_names_norm,
  null as references,
  lower(c.doi) as clean_doi
from gcp_cset_links.arxiv_metadata_norm a
left join
gcp_cset_arxiv_metadata.arxiv_metadata_latest c
on a.id = c.id