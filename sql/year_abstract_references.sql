-- find articles that match on normalized references, normalized abstract, and year
select a.id as all1_id, m.id as all2_id
from {{params.dataset}}.all_metadata_norm a
inner join
{{params.dataset}}.all_metadata_norm m
on ((a.abstract_norm = m.abstract_norm) and
    (a.abstract_norm is not null) and (a.abstract_norm != "") and
    (a.year = m.year) and (a.year is not null) and
    (a.references = m.references) and (a.references is not null) and (a.references != ""))
