-- get author last names
select
  id, array_agg(last_name IGNORE NULLS) as last_names
from gcp_cset_clarivate.wos_summary_names_latest
where role="author"
group by id
