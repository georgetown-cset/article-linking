-- get cnki years, titles, and authors
select
  a.cnki_document_id as id,
  cast(b.year as int64) as year,
  lower(b.cnki_doi) as clean_doi,
  split(b.author, ";") as last_names
from gcp_cset_cnki.cset_cnki_journals_corpus b
inner join
  gcp_cset_cnki.cset_cnki_journals_id_mappings a
on (a.document_name = b.document_name) and ((a.cnki_doi = b.cnki_doi) or (a.cnki_doi is null))
union all
select
  a.cnki_document_id as id,
  b.publication_year as year,
  lower(b.cnki_doi) as clean_doi,
  split(b.author, ";") as last_names
from gcp_cset_cnki.cset_cnki_dissertations_corpus b
inner join
  gcp_cset_cnki.cset_cnki_dissertations_id_mappings a
on (a.document_name = b.document_name) and ((a.cnki_doi = b.cnki_doi) or (a.cnki_doi is null))
union all
select
  a.cnki_document_id as id,
  b.publication_year as year,
  lower(b.doi) as clean_doi,
  split(b.author, ";") as last_names
from gcp_cset_cnki.cnki_conferences b
inner join
  gcp_cset_cnki.cset_cnki_conferences_id_mappings a
on (a.document_name = b.document_name) and ((a.cnki_doi = b.doi) or (a.cnki_doi is null))