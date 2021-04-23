-- map references to their carticle ids
select distinct b.merged_id as id, c.merged_id as ref_id from
(select id, reference from (
  select id, reference, dataset from staging_gcp_cset_links.all_metadata_with_cld2_lid cross join unnest(split(references, ",")) as reference
  union all
  select id, ref_id as reference, "cnki" as dataset from cnki_citation_linkage.exact_normalized_title_matches
  union all
  (
    select a12.cnki_document_id as id, a11.target_id as reference, "cnki" as dataset from
    cnki_citation_graph_v1.cset_cnki_journals_out_citations_en_simhash_sts a11
    left join
    gcp_cset_cnki.cset_cnki_journals_id_mappings a12
    on a11.source_document_name = a12.document_name
  )
  union all
  (
    select a22.cnki_document_id as id, a23.cnki_document_id as reference, "cnki" as dataset from
    cnki_citation_graph_v1.cset_cnki_journals_out_citations_zh_sts a21
    left join
    gcp_cset_cnki.cset_cnki_journals_id_mappings a22
    on a22.document_name = a21.document_name
    left join
    gcp_cset_cnki.cset_cnki_journals_id_mappings a23
    on a23.document_name = a21.out_citation_document_name
  )
) where reference in (
  select orig_id from staging_gcp_cset_links.article_links_with_dataset
)) a
left join staging_gcp_cset_links.article_links b
on a.id = b.orig_id
left join staging_gcp_cset_links.article_links c
on a.reference = c.orig_id
where (b.merged_id is not null) and (c.merged_id is not null)