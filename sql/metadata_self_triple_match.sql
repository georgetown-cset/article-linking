-- get combined set of metadata matches
select distinct * from (
select * from {{staging_dataset}}.year_title_abstract
union all
select * from {{staging_dataset}}.year_title_author
union all
select * from {{staging_dataset}}.year_title_references
union all
select * from {{staging_dataset}}.year_title_doi
union all
select * from {{staging_dataset}}.year_abstract_author
union all
select * from {{staging_dataset}}.year_abstract_references
union all
select * from {{staging_dataset}}.year_abstract_doi
union all
select * from {{staging_dataset}}.year_author_references
union all
select * from {{staging_dataset}}.year_author_doi
union all
select * from {{staging_dataset}}.year_references_doi
union all
select * from {{staging_dataset}}.title_abstract_author
union all
select * from {{staging_dataset}}.title_abstract_references
union all
select * from {{staging_dataset}}.title_abstract_doi
union all
select * from {{staging_dataset}}.title_author_references
union all
select * from {{staging_dataset}}.title_author_doi
union all
select * from {{staging_dataset}}.title_references_doi
union all
select * from {{staging_dataset}}.abstract_author_references
union all
select * from {{staging_dataset}}.abstract_author_doi
union all
select * from {{staging_dataset}}.abstract_references_doi
union all
select * from {{staging_dataset}}.author_references_doi
union all
select * from {{staging_dataset}}.arxiv_id_match
)