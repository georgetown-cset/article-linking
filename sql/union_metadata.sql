select cast(id as string) as id, title, abstract, clean_doi, year, last_names, null as references,
  "arxiv" as dataset
  from {{params.dataset}}.arxiv_metadata
UNION ALL
select cast(id as string) as id, title, abstract, clean_doi, cast(year as int64), last_names, references,
  "wos" as dataset
  from {{params.dataset}}.wos_metadata
UNION ALL
select cast(id as string) as id, title, abstract, clean_doi, cast(year as int64), last_names, references,
  "ds" as dataset
  from {{params.dataset}}.ds_metadata
UNION ALL
select cast(id as string) as id, title, abstract, clean_doi, cast(year as int64), last_names, references,
  "mag" as dataset
from {{params.dataset}}.mag_metadata
UNION ALL
select cast(id as string) as id, title, abstract, clean_doi, cast(year as int64), last_names, null as references,
  "cnki" as dataset
from {{params.dataset}}.cnki_metadata