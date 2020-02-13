select
  id as ds_id,
  array_to_string(array(select r from unnest(references) as r order by r), ",") as references
from gcp_cset_digital_science.dimensions_publications_latest