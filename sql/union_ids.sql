-- glue all the ids together (used in validation)
select id from {{staging_dataset}}.arxiv_ids
UNION ALL
select cast(id as string) from {{staging_dataset}}.mag_ids
UNION ALL
select id from {{staging_dataset}}.wos_ids
UNION ALL
select id from {{staging_dataset}}.ds_ids
UNION ALL
select id from {{staging_dataset}}.cnki_ids
UNION ALL
select id from {{staging_dataset}}.papers_with_code_ids
UNION ALL
select id from {{staging_dataset}}.openalex_ids
UNION ALL
select id from {{staging_dataset}}.s2_ids