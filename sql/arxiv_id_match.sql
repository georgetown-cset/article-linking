-- papers with code.papers_with_abstracts has an arxiv_id column that is often not null; use this to match
-- on arxiv where possible
with arxiv_pwc_mapping as (
  select
    arxiv.id as id1,
    pwc.paper_url as id2
  from
    gcp_cset_arxiv_metadata.arxiv_metadata_latest arxiv
  inner join
    papers_with_code.papers_with_abstracts pwc
  on arxiv.id = pwc.arxiv_id
)

select id1 as all1_id, id2 as all2_id from arxiv_pwc_mapping
union all
select id2 as all1_id, id1 as all2_id from arxiv_pwc_mapping