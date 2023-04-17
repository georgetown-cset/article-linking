-- map references to their carticle ids
select distinct links1.merged_id as id, links2.merged_id as ref_id from
(select id, reference from {{ staging_dataset }}.all_metadata_with_cld2_lid cross join unnest(split(references, ",")) as reference
  where reference in (
    select orig_id from {{ staging_dataset }}.article_links_with_dataset
)) as references
left join {{ staging_dataset }}.article_links as links1
on references.id = links1.orig_id
left join {{ staging_dataset }}.article_links as links2
on references.reference = links2.orig_id
where (links1.merged_id is not null) and (links2.merged_id is not null)
