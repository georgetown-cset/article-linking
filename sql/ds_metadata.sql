-- get digital science combined metadata used in matching
with no_doi as (
  SELECT
    d.id,
    d.title,
    d.abstract,
    d.year,
    a.last_names,
    array_to_string(array(select r from unnest(d.references) as r order by r), ",") as references
  FROM
    gcp_cset_digital_science.dimensions_publications_with_abstracts_latest d
  INNER JOIN
    {{ staging_dataset }}.ds_authors a
  ON
    d.id = a.id
),

with_doi as (
  select
    id,
    no_doi.title,
    no_doi.abstract,
    resulting_publication_doi as clean_doi,
    no_doi.year,
    last_names,
    no_doi.references
  from no_doi
  left join
  gcp_cset_digital_science.dimensions_publications_with_abstracts_latest
  using(id)
  union all
  select
    id,
    no_doi.title,
    no_doi.abstract,
    doi as clean_doi,
    no_doi.year,
    last_names,
    no_doi.references
  from no_doi
  left join
  gcp_cset_digital_science.dimensions_publications_with_abstracts_latest
  using(id)
)

select
  id,
  title,
  abstract,
  clean_doi,
  year,
  last_names,
  references
from with_doi
union all
select
  id,
  title,
  abstract,
  clean_doi,
  year,
  last_names,
  orig_id_references.references
from with_doi
left join
{{ staging_dataset }}.orig_id_references
on with_doi.id = orig_id_references.orig_id
