-- get cnki years, titles, and authors
select
  a.cnki_document_id as id,
  b.year,
  lower(b.cnki_doi) as clean_doi,
  split(b.author, ";") as last_names
from gcp_cset_cnki.cset_cnki_journals_corpus b
inner join
  gcp_cset_cnki.cset_cnki_journals_id_mappings a
on (a.document_name = b.document_name) and ((a.cnki_doi = b.cnki_doi) or (a.cnki_doi is null))