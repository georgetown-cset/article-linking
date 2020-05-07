select id from  from {{params.dataset}}.arxiv_ids
UNION ALL
select id from  from {{params.dataset}}.mag_ids
UNION ALL
select id from  from {{params.dataset}}.wos_ids
UNION ALL
select id from  from {{params.dataset}}.ds_ids
UNION ALL
select id from  from {{params.dataset}}.cnki_ids