select a.id as {TABLE1}1_id, m.id as {TABLE1}2_id
from {DATASET}.{TABLE1}_metadata_norm a
inner join
{DATASET}.{TABLE1}_metadata_norm m
on ((a.year = m.year) and (a.year is not null) and
   (a.title_norm = m.title_norm) and (m.title_norm is not null) and (a.title_norm != ""))