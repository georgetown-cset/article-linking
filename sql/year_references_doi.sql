-- find articles that match on year, doi, and references
select a.id as all1_id, m.id as all2_id
from {{params.dataset}}.all_metadata_norm a
inner join
{{params.dataset}}.all_metadata_norm m
on ((a.year = m.year) and (a.year is not null) and
    (a.references = m.references) and (a.references is not null) and (a.references != "") and
    (a.clean_doi = m.clean_doi) and (a.clean_doi is not null) and (a.clean_doi != ""))
