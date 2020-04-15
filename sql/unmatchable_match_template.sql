(select id as all1_id, id as all2_id from {{params.dataset}}.all_metadata
where id not in (select all1_id from {{params.dataset}}.all_all_all_match_pairs))
union all
(select * from {{params.dataset}}.all_all_all_match_pairs)