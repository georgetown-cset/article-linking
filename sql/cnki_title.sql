(select
  a.cnki_id as id,
  b.publication_title as title
from gcp_cset_cnki.cset_cnki_journals_corpus b
inner join
gcp_cset_cnki.cset_cnki_journals_id_mappings a
on (a.document_name = b.document_name) and ((a.cnki_doi = b.cnki_doi) or (a.cnki_doi is null)))
union all
(select
  a.cnki_id as id,
  b.publication_title_en as title
from gcp_cset_cnki.cset_cnki_journals_corpus b
inner join
gcp_cset_cnki.cset_cnki_journals_id_mappings a
on (a.document_name = b.document_name) and ((a.cnki_doi = b.cnki_doi) or (a.cnki_doi is null)))