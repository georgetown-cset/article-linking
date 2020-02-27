select id, clean_doi from {DATASET}.wos_unambiguous_dois_pre
where
  ((clean_doi is not null) and (clean_doi in (select clean_doi from (select clean_doi from
    (select clean_doi, count(id) as num_ids from {DATASET}.wos_unambiguous_dois_pre
    where clean_doi is not null group by clean_doi)
  where num_ids = 1))))
and
  ((id is not null) and (id in (select id from (select id from
    (select id, count(clean_doi) as num_dois from {DATASET}.wos_unambiguous_dois_pre
    where id is not null group by id)
  where num_dois = 1))))