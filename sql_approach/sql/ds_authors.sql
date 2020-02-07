select
  id,
  ARRAY(select last_name from UNNEST(author_affiliations)) as last_name
from gcp_cset_digital_science.dimensions_publications_latest