-- glue all the metadata together into one table
select cast(id as string) as id, title, abstract, clean_doi, cast(year as int64) as year, last_names,
  references, "arxiv" as dataset
  from {{staging_dataset}}.arxiv_metadata
UNION ALL
select cast(id as string) as id, title, abstract, clean_doi, cast(year as int64) as year, last_names,
  references, "wos" as dataset
  from {{staging_dataset}}.wos_metadata
UNION ALL
select cast(id as string) as id, title, abstract, clean_doi, cast(year as int64) as year, last_names,
  references, "ds" as dataset
  from {{staging_dataset}}.ds_metadata
UNION ALL
select cast(id as string) as id, title, abstract, clean_doi, cast(year as int64) as year, last_names,
  references, "mag" as dataset
from {{staging_dataset}}.mag_metadata
UNION ALL
select cast(id as string) as id, title, abstract, clean_doi, cast(year as int64) as year, last_names,
  references, "cnki" as dataset
from {{staging_dataset}}.cnki_metadata
UNION ALL
select cast(id as string) as id, title, abstract, null as clean_doi, cast(year as int64) as year, last_names,
  references, "pwc" as dataset
from {{staging_dataset}}.papers_with_code_metadata
UNION ALL
select id, title, abstract, clean_doi, year, last_names,
  references, "openalex" as dataset
from {{staging_dataset}}.openalex_metadata