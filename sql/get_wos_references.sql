select
  id as wos_id,
  string_agg(ref_id order by ref_id) as references
from gcp_cset_clarivate.wos_references_latest group by id