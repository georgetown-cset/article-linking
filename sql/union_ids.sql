-- glue all the ids together (used in validation)
select distinct id from {{ staging_dataset }}.arxiv_ids
UNION ALL
select distinct cast(id as string) from {{ staging_dataset }}.mag_ids
UNION ALL
select distinct id from {{ staging_dataset }}.wos_ids
UNION ALL
select distinct id from {{ staging_dataset }}.ds_ids
UNION ALL
select distinct id from {{ staging_dataset }}.cnki_ids
UNION ALL
select distinct id from {{ staging_dataset }}.papers_with_code_ids
UNION ALL
select distinct id from {{ staging_dataset }}.openalex_ids