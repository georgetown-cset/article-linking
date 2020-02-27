select id, lower(doi) as clean_doi from gcp_cset_arxiv_metadata.arxiv_metadata_latest
where
  ((doi is not null) and (doi in (select doi from (select doi from
    (select lower(doi) as doi, count(id) as num_ids from gcp_cset_arxiv_metadata.arxiv_metadata_latest
    where doi is not null group by doi)
  where num_ids = 1))))
and
  ((id is not null) and (id in (select id from (select id from
    (select id, count(lower(doi)) as num_dois from gcp_cset_arxiv_metadata.arxiv_metadata_latest
    where id is not null group by id)
  where num_dois = 1))))