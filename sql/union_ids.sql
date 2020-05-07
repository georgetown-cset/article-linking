-- glue all the ids together (used in validation)
select id from {{params.dataset}}.arxiv_ids
UNION ALL
select cast(id as string) from {{params.dataset}}.mag_ids
UNION ALL
select id from {{params.dataset}}.wos_ids
UNION ALL
select id from {{params.dataset}}.ds_ids
UNION ALL
select id from {{params.dataset}}.cnki_ids