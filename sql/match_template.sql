-- find articles that match on one of the stronger indicators (title, abstract, doi, references) and one other indicator
select a.id as all1_id, m.id as all2_id
from {{staging_dataset}}.all_metadata_norm_filt as a
inner join
{{staging_dataset}}.all_metadata_norm_filt as m
on (a.{{ params.strong }} = m.{{ params.strong }}) and
    (a.{{ params.strong }} is not null) and (a.{{ params.strong }} != "") and
    (a.{{ params.other }} = m.{{ params.other }}) and (a.{{ params.other }} is not null) {{params.additional_checks}}
