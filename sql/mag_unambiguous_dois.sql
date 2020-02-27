select PaperId as id, lower(Doi) as clean_doi from gcp_cset_mag.Papers
where
  (Doi in (select Doi from (select Doi, count(Doi) as num_dois from gcp_cset_mag.Papers group by Doi)
  where num_dois = 1))
and
  (PaperId in (select PaperId from (select PaperId, count(PaperId) as num_ids from gcp_cset_mag.Papers group by PaperId)
  where num_ids = 1))