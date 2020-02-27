select
  a.id,
  a.year,
  a.title_norm,
  a.abstract_trunc_norm_len_filt,
  a.last_names_norm,
  b.references,
  lower(c.Doi) as clean_doi
from gcp_cset_links.mag_metadata_norm a
left join
tmp_add_references_to_metadata.mag_references b
on a.id = b.mag_id
left join
gcp_cset_mag.Papers c
on a.id = c.PaperId