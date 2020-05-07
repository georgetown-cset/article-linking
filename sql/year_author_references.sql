-- find articles that match on normalized author last names, normalized year, and references
select a.id as all1_id, m.id as all2_id
from {{params.dataset}}.all_metadata_norm a
inner join
{{params.dataset}}.all_metadata_norm m
on ((a.last_names_norm = m.last_names_norm) and (m.last_names_norm is not null) and (a.last_names_norm != "") and
    (a.year = m.year) and (a.year is not null) and
    (a.references = m.references) and (a.references is not null) and (a.references != ""))
