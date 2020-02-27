select id, lower(doi) as clean_doi from gcp_cset_digital_science.dimensions_publications_with_abstracts_latest
where
  (doi in (select doi from (select doi, count(doi) as num_dois
    from gcp_cset_digital_science.dimensions_publications_with_abstracts_latest group by doi)
  where num_dois = 1))
and
  (id in (select id from (select id, count(id) as num_ids
    from gcp_cset_digital_science.dimensions_publications_with_abstracts_latest group by id)
  where num_ids = 1))