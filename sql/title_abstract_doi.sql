-- find articles that match on normalized title, normalized abstract, and doi
select a.id as all1_id, m.id as all2_id
from {{staging_dataset}}.all_metadata_norm_filt a
inner join
{{staging_dataset}}.all_metadata_norm_filt m
on ((a.title_norm = m.title_norm) and (a.title_norm is not null) and (a.title_norm != "") and
    (a.abstract_norm = m.abstract_norm) and
    (a.abstract_norm is not null) and (a.abstract_norm != "") and
    (a.clean_doi = m.clean_doi) and (a.clean_doi is not null) and (a.clean_doi != ""))
