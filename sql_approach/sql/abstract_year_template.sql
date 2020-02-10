select a.id as {TABLE1}_id, m.id as {TABLE2}_id
from {DATASET}.{TABLE1}_metadata_norm a
inner join
{DATASET}.{TABLE2}_metadata_norm m
on ((a.year = m.year) and (a.year is not null) and
   (a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and
   (m.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != ""))
