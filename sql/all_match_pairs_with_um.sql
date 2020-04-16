(select id as all1_id, id as all2_id from {{params.dataset}}.all_metadata_norm
where id not in (select all1_id from {{params.dataset}}.metadata_self_triple_match))
union all
(select * from {{params.dataset}}.metadata_self_triple_match)