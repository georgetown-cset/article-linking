-- find articles that match on normalized author last names, normalized abstract, and normalized title
select a.id as all1_id, m.id as all2_id
from {{staging_dataset}}.all_metadata_norm a
inner join
{{staging_dataset}}.all_metadata_norm m
on ((a.title_norm = m.title_norm) and (a.title_norm is not null) and (a.title_norm != "") and
    (a.abstract_norm = m.abstract_norm) and
    (a.abstract_norm is not null) and (a.abstract_norm != "") and
    (a.last_names_norm = m.last_names_norm) and (m.last_names_norm is not null) and (a.last_names_norm != ""))
