-- add "self matches" for the articles that didn't match anything (this can happen if the article has a lot of null
-- fields) to the rest of the article match pairs
(select id as id1, id as id2 from {{staging_dataset}}.all_metadata_norm
where id not in (select all1_id from {{staging_dataset}}.metadata_self_triple_match))
union all
(select all1_id as id1, all2_id as id2 from {{staging_dataset}}.metadata_self_triple_match)