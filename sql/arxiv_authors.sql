-- get arxiv author last names
select
  id,
  ARRAY(select keyname from UNNEST(authors.author)) as last_names
from gcp_cset_arxiv_metadata.arxiv_metadata_latest
