-- get wos abstracts. Note that there may be multiple versions of the abstracts (see abstract_id)
SELECT id, STRING_AGG(paragraph_text, "\n" ORDER BY CAST(paragraph_id AS INT64) ASC) as abstract
FROM gcp_cset_clarivate.wos_abstract_paragraphs_latest
GROUP BY id, abstract_id;