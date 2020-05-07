-- get cnki metadata used for matching
select
  a.id,
  b.title,
  c.abstract,
  a.clean_doi,
  a.year,
  a.last_names
from
  {{params.dataset}}.cnki_year_doi_authors a
inner join
  {{params.dataset}}.cnki_title b
on a.id = b.id
inner join
  {{params.dataset}}.cnki_abstract c
on a.id = c.id