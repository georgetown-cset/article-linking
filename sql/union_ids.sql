-- glue all the ids together (used in validation)
select id from {{ staging_dataset }}.arxiv_ids
UNION ALL
select id from {{ staging_dataset }}.wos_ids
UNION ALL
select id from {{ staging_dataset }}.papers_with_code_ids
UNION ALL
select id from {{ staging_dataset }}.openalex_ids
UNION ALL
select id from {{ staging_dataset }}.s2_ids
UNION ALL
select id from {{ staging_dataset }}.lens_ids
