select distinct * from (
select * from {DATASET}.{TABLE1}_{TABLE1}_year_title_abstract
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_year_title_author
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_year_title_references
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_year_title_doi
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_year_abstract_author
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_year_abstract_references
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_year_abstract_doi
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_year_author_references
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_year_author_doi
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_year_references_doi
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_title_abstract_author
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_title_abstract_references
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_title_abstract_doi
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_title_author_references
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_title_author_doi
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_title_references_doi
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_abstract_author_references
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_abstract_author_doi
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_abstract_references_doi
union all
select * from {DATASET}.{TABLE1}_{TABLE1}_author_references_doi
)