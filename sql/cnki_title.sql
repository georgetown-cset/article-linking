-- get both the chinese and english versions of article titles
(select
  a.cnki_document_id as id,
  b.publication_title as title
from gcp_cset_cnki.cset_cnki_journals_corpus b
inner join
gcp_cset_cnki.cset_cnki_journals_id_mappings a
on (a.document_name = b.document_name) and ((a.cnki_doi = b.cnki_doi) or (a.cnki_doi is null) or (trim(a.cnki_doi) = "")))
union all
(select
  a.cnki_document_id as id,
  b.publication_title_en as title
from gcp_cset_cnki.cset_cnki_journals_corpus b
inner join
gcp_cset_cnki.cset_cnki_journals_id_mappings a
on (a.document_name = b.document_name) and ((a.cnki_doi = b.cnki_doi) or (a.cnki_doi is null) or (trim(a.cnki_doi) = "")))
union all
(select
  a.cnki_document_id as id,
  b.publication_title as title
from gcp_cset_cnki.cset_cnki_dissertations_corpus b
inner join
gcp_cset_cnki.cset_cnki_dissertations_id_mappings a
on (a.document_name = b.document_name) and ((a.cnki_doi = b.cnki_doi) or (a.cnki_doi is null) or (trim(a.cnki_doi) = "")))
union all
(select
  a.cnki_document_id as id,
  b.publication_title_en as title
from gcp_cset_cnki.cset_cnki_dissertations_corpus b
inner join
gcp_cset_cnki.cset_cnki_dissertations_id_mappings a
on (a.document_name = b.document_name) and ((a.cnki_doi = b.cnki_doi) or (a.cnki_doi is null) or (trim(a.cnki_doi) = "")))
union all
(select
  a.cnki_document_id as id,
  b.publication_title as title
from gcp_cset_cnki.cnki_conferences b
inner join
gcp_cset_cnki.cset_cnki_conferences_id_mappings a
on (a.document_name = b.document_name) and ((a.cnki_doi = b.doi) or (a.cnki_doi is null) or (trim(a.cnki_doi) = "")))
union all
(select
  a.cnki_document_id as id,
  b.publication_title_en as title
from gcp_cset_cnki.cnki_conferences b
inner join
gcp_cset_cnki.cset_cnki_conferences_id_mappings a
on (a.document_name = b.document_name) and ((a.cnki_doi = b.doi) or (a.cnki_doi is null) or (trim(a.cnki_doi) = "")))