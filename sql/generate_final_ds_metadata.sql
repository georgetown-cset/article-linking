select
  a.id,
  a.year,
  a.title_norm,
  a.abstract_trunc_norm_len_filt,
  a.last_names_norm,
  b.references,
  lower(c.doi) as clean_doi
from gcp_cset_links.ds_metadata_norm a
left join
tmp_add_references_to_metadata.ds_references b
on a.id = b.ds_id
left join
gcp_cset_digital_science.dimensions_publications_latest c
on a.id = c.id