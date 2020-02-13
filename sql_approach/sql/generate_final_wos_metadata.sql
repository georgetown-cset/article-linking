select
  a.id,
  a.year,
  a.title_norm,
  a.abstract_trunc_norm_len_filt,
  a.last_names_norm,
  b.references,
  c.clean_doi
from gcp_cset_links.wos_metadata_norm a
left join
tmp_add_references_to_metadata.wos_references b
on a.id = b.wos_id
left join
gcp_cset_links.wos_unambiguous_dois_pre c
on a.id = c.id