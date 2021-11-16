-- get cnki ids (used in validation). Formerly, we got these from gcp_cset_cnki.cset_cnki_journals_id_mappings,
-- but per conversation with Daniel, not all these ids will appear in any of the three metadata tables (below)
-- we use to construct CNKI articles. So instead, we'll union the ids that appear in the metadata tables we use.
select distinct(id) from (
  select id from {{staging_dataset}}.cnki_year_doi_authors
  union all
  select id from {{staging_dataset}}.cnki_title
  union all
  select id from {{staging_dataset}}.cnki_abstract
)
