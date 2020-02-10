select a.id as {SMALL_TABLE}_id, m.id as {LARGE_TABLE}_id
from {DATASET}.{SMALL_TABLE}_metadata_norm a
inner join
{DATASET}.{LARGE_TABLE}_metadata_norm m
on ((a.title_norm = m.title_norm) and (a.title_norm is not null) and (a.title_norm != "") and
   (a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and
   (m.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != ""))