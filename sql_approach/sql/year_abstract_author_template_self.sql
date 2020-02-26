select a.id as {TABLE1}1_id, m.id as {TABLE2}2_id
from {DATASET}.{TABLE1}_metadata_norm a
inner join
{DATASET}.{TABLE2}_metadata_norm m
on ((a.abstract_norm = m.abstract_norm) and
    (a.abstract_norm is not null) and (a.abstract_norm != "") and
    (a.last_names_norm = m.last_names_norm) and (m.last_names_norm is not null) and (a.last_names_norm != "") and
    (a.year = m.year) and (a.year is not null))
