-- get combined set of metadata matches
select distinct * from (
select * from {{params.dataset}}.year_title_abstract
union all
select * from {{params.dataset}}.year_title_author
union all
select * from {{params.dataset}}.year_title_references
union all
select * from {{params.dataset}}.year_title_doi
union all
select * from {{params.dataset}}.year_abstract_author
union all
select * from {{params.dataset}}.year_abstract_references
union all
select * from {{params.dataset}}.year_abstract_doi
union all
select * from {{params.dataset}}.year_author_references
union all
select * from {{params.dataset}}.year_author_doi
union all
select * from {{params.dataset}}.year_references_doi
union all
select * from {{params.dataset}}.title_abstract_author
union all
select * from {{params.dataset}}.title_abstract_references
union all
select * from {{params.dataset}}.title_abstract_doi
union all
select * from {{params.dataset}}.title_author_references
union all
select * from {{params.dataset}}.title_author_doi
union all
select * from {{params.dataset}}.title_references_doi
union all
select * from {{params.dataset}}.abstract_author_references
union all
select * from {{params.dataset}}.abstract_author_doi
union all
select * from {{params.dataset}}.abstract_references_doi
union all
select * from {{params.dataset}}.author_references_doi
)