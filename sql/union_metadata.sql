select cast(id as string) as id, title, abstract, clean_doi, year, last_names, null as references,
  "arxiv" as dataset
  from {DATASET}.arxiv_metadata
UNION ALL
select cast(id as string) as id, title, abstract, clean_doi, cast(year as int64), last_names, references,
  "wos" as dataset
  from {DATASET}.wos_metadata
UNION ALL
select cast(id as string) as id, title, abstract, clean_doi, cast(year as int64), last_names, references,
  "ds" as dataset
  from {DATASET}.ds_metadata
UNION ALL
select cast(id as string) as id, title, abstract, clean_doi, cast(year as int64), last_names, references,
  "mag" as dataset
from {DATASET}.mag_metadata
UNION ALL
select cast(id as string) as id, title, abstract, clean_doi, cast(year as int64), last_names, references,
  "cnki" as dataset
from {DATASET}.cnki_metadata