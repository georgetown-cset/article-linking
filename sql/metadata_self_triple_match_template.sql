select distinct * from (
select * from {{params.dataset}}.all_all_year_title_abstract
union all
select * from {{params.dataset}}.all_all_year_title_author
union all
select * from {{params.dataset}}.all_all_year_title_references
union all
select * from {{params.dataset}}.all_all_year_title_doi
union all
select * from {{params.dataset}}.all_all_year_abstract_author
union all
select * from {{params.dataset}}.all_all_year_abstract_references
union all
select * from {{params.dataset}}.all_all_year_abstract_doi
union all
select * from {{params.dataset}}.all_all_year_author_references
union all
select * from {{params.dataset}}.all_all_year_author_doi
union all
select * from {{params.dataset}}.all_all_year_references_doi
union all
select * from {{params.dataset}}.all_all_title_abstract_author
union all
select * from {{params.dataset}}.all_all_title_abstract_references
union all
select * from {{params.dataset}}.all_all_title_abstract_doi
union all
select * from {{params.dataset}}.all_all_title_author_references
union all
select * from {{params.dataset}}.all_all_title_author_doi
union all
select * from {{params.dataset}}.all_all_title_references_doi
union all
select * from {{params.dataset}}.all_all_abstract_author_references
union all
select * from {{params.dataset}}.all_all_abstract_author_doi
union all
select * from {{params.dataset}}.all_all_abstract_references_doi
union all
select * from {{params.dataset}}.all_all_author_references_doi
)