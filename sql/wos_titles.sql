select
  id,
  title_id as title_id,
  title as title
from gcp_cset_clarivate.wos_titles_latest
where title_type="item"
