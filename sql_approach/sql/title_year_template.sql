select a.id as {SMALL_TABLE}_id, m.id as {LARGE_TABLE}_id
from {DATASET}.{SMALL_TABLE}_metadata_norm a
inner join
{DATASET}.{LARGE_TABLE}_metadata_norm m
on ((a.year = m.year) and (a.year is not null) and
   (a.title_norm = m.title_norm) and (m.title_norm is not null) and (a.title_norm != ""))