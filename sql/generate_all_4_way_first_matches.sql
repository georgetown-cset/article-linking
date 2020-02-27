select distinct * from
(select * from {DATASET}.arxiv_4_way_first
union all
select * from {DATASET}.wos_4_way_first
union all
select * from {DATASET}.ds_4_way_first
union all
select * from {DATASET}.mag_4_way_first)
