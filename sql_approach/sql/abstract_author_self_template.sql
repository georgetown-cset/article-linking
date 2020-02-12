select a.id as {TABLE1}1_id, m.id as {TABLE1}2_id
from {DATASET}.{TABLE1}_metadata_norm a
inner join
{DATASET}.{TABLE1}_metadata_norm m
on ((a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and
    (a.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != "") and
    (a.last_names_norm = m.last_names_norm) and (m.last_names_norm is not null) and (a.last_names_norm != ""))