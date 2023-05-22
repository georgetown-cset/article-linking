-- find articles that match on normalized author last names, references, and doi
select a.id as all1_id, m.id as all2_id
from {{staging_dataset}}.all_metadata_norm_filt a
inner join
{{staging_dataset}}.all_metadata_norm_filt m
on ((a.last_names_norm = m.last_names_norm) and (m.last_names_norm is not null) and (a.last_names_norm != "") and
    (a.references = m.references) and (a.references is not null) and (a.references != "") and
    (a.clean_doi = m.clean_doi) and (a.clean_doi is not null) and (a.clean_doi != ""))
