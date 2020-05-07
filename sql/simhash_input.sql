-- get input for simhash, which is articles with not null titles and abstracts that have not already been matched
select
  id,
  year,
  concat(title_norm, abstract_norm) as normalized_text
from {{params.staging_dataset}}.all_metadata_norm
where
    (year is not null) and
    (title_norm is not null) and (title_norm != "") and
    (abstract_norm is not null) and (abstract_norm != "") and
    id not in (
      select a.id from {{params.staging_dataset}}.all_metadata_norm a
      left join
      {{params.production_dataset}}.all_metadata_with_cld2_lid b
      on a.id = b.id
      where (a.title = b.title) and (a.abstract = b.abstract) and (a.year = b.year) and (a.title != "") and
      (a.title is not null) and (a.abstract != "") and (a.abstract is not null) and (a.year is not null)
    )