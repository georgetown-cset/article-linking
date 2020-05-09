-- get cnki ids (used in validation)
select distinct(cnki_document_id) as id from gcp_cset_cnki.cset_cnki_journals_id_mappings