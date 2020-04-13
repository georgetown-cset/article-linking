(select
  a.cnki_id as id,
  b.publication_title as title
from gcp_cset_cnki.cset_cnki_journals_corpus b
inner join
gcp_cset_cnki.cset_cnki_journals_id_mappings_test_1 a
on (a.document_name = b.document_name) and (a.cnki_doi = b.cnki_doi))
union all
(select
  a.cnki_id as id,
  b.publication_title_en as title
from gcp_cset_cnki.cset_cnki_journals_corpus b
inner join
gcp_cset_cnki.cset_cnki_journals_id_mappings_test_1 a
on (a.document_name = b.document_name) and (a.cnki_doi = b.cnki_doi))