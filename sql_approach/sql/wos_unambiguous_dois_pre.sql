select id, lower(identifier_value) as clean_doi from gcp_cset_clarivate.wos_dynamic_identifiers_latest where
(identifier_type="doi") and (identifier_value is not null)
