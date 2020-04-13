select
  a.id,
  b.title,
  c.abstract,
  a.clean_doi,
  a.year,
  a.last_names,
  null as references
from
  {{params.dataset}}.cnki_year_doi_authors a
inner join
  {{params.dataset}}.cnki_title b
on a.id = b.id
inner join
  {{params.dataset}}.cnki_abstract c
on a.id = c.id