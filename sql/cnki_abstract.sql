-- Get both the english and chinese versions of cnki abstracts. Join is needed to map the non-unique
-- cnki identifiers to our internal IDs.

(select
  a.cnki_document_id as id,
  b.abstract
from gcp_cset_cnki.cset_cnki_journals_corpus b
inner join
gcp_cset_cnki.cset_cnki_journals_id_mappings a
-- For uniqueness, match on both document_name and cnki_doi. Unlike cnki_doi, document_name is never null. 
-- Fall back to matching document name alone if cnki_doi is null
on (a.document_name = b.document_name) and ((a.cnki_doi = b.cnki_doi) or (a.cnki_doi is null)))
union all
(select
  a.cnki_document_id as id,
  b.abstract_en as abstract
from gcp_cset_cnki.cset_cnki_journals_corpus b
inner join
gcp_cset_cnki.cset_cnki_journals_id_mappings a
on (a.document_name = b.document_name) and ((a.cnki_doi = b.cnki_doi) or (a.cnki_doi is null)))
