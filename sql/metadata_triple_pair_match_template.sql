select distinct * from (select * from {DATASET}.{TABLE1}_{TABLE2}_abstract_author_year
union all
select * from {DATASET}.{TABLE1}_{TABLE2}_abstract_author_title
union all
select * from {DATASET}.{TABLE1}_{TABLE2}_abstract_title_year
union all
select * from {DATASET}.{TABLE1}_{TABLE2}_author_title_year)
