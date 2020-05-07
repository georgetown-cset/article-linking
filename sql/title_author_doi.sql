-- find articles that match on normalized author last names, normalized title, and doi
select a.id as all1_id, m.id as all2_id
from {{params.dataset}}.all_metadata_norm a
inner join
{{params.dataset}}.all_metadata_norm m
on ((a.title_norm = m.title_norm) and (a.title_norm is not null) and (a.title_norm != "") and
    (a.last_names_norm = m.last_names_norm) and (m.last_names_norm is not null) and (a.last_names_norm != "") and
    (a.clean_doi = m.clean_doi) and (a.clean_doi is not null) and (a.clean_doi != ""))
