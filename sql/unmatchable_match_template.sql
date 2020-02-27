(select id as {TABLE1}1_id, id as {TABLE1}2_id from {DATASET}.{TABLE1}_metadata
where id not in (select all1_id from {DATASET}.{TABLE1}_{TABLE1}_{TABLE1}_match_pairs))
union all
(select * from {DATASET}.{TABLE1}_{TABLE1}_{TABLE1}_match_pairs)