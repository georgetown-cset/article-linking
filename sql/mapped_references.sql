select distinct b.merged_id as id, c.merged_id as ref_id from
(select id, reference from (
  select id, reference, dataset from {{params.dataset}}.all_metadata_with_cld2_lid cross join unnest(split(references, ",")) as reference
  union all
  (
    select a12.cnki_id as id, a11.target_id as reference, "cnki" as dataset from
    cnki_citation_graph_v1.cset_cnki_journals_out_citations_en_simhash_sts a11
    left join
    gcp_cset_cnki.cset_cnki_journals_id_mappings a12
    on a11.source_document_name = a12.document_name
  )
  union all
  (
    select a22.cnki_id as id, a23.cnki_id as reference, "cnki" as dataset from
    cnki_citation_graph_v1.cset_cnki_journals_out_citations_zh_sts a21
    left join
    gcp_cset_cnki.cset_cnki_journals_id_mappings a22
    on a22.document_name = a21.document_name
    left join
    gcp_cset_cnki.cset_cnki_journals_id_mappings a23
    on a23.document_name = a21.out_citation_document_name
  )
) where reference in (
  select orig_id from {{params.dataset}}.article_links_with_dataset where dataset=dataset
)) a
left join {{params.dataset}}.article_links b
on a.id = b.orig_id
left join {{params.dataset}}.article_links c
on a.reference = c.orig_id
