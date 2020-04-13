select
  a.id,
  b.title,
  c.abstract,
  a.clean_doi,
  a.year,
  a.last_names,
  null as references
from
  {DATASET}.cnki_year_doi_authors a
inner join
  {DATASET}.cnki_title b
on a.id = b.id
inner join
  {DATASET}.cnki_abstract c
on a.id = c.id