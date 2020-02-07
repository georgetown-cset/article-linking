select
  id,
  ARRAY(select keyname from UNNEST(authors.author)) as last_name
from gcp_cset_arxiv_metadata.arxiv_metadata_latest