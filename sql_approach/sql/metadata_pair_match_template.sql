select distinct * from (select * from {DATASET}.{TABLE1}_{TABLE2}_title_year
union all
select * from {DATASET}.{TABLE1}_{TABLE2}_abstract_year
union all
select * from {DATASET}.{TABLE1}_{TABLE2}_abstract_title
union all
select * from {DATASET}.{TABLE1}_{TABLE2}_abstract_author
union all
select * from {DATASET}.{TABLE1}_{TABLE2}_title_author)