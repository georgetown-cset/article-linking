select
  id,
  MIN(title_id) as title_id,
  MIN(title) as title
from gcp_cset_clarivate.wos_titles_latest
where title_type="item"
group by id