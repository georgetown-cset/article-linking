select a.id as {TABLE1}_id, m.id as {TABLE2}_id
from gcp_cset_links.{TABLE1}_metadata_norm a
inner join
gcp_cset_links.{TABLE2}_metadata_norm m
on ((a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and
    (a.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != "") and
    (a.last_names_norm = m.last_names_norm) and (m.last_names_norm is not null) and (a.last_names_norm != "") and
    (a.references = m.references) and (a.references is not null) and (a.references != ""))
