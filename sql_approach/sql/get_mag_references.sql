select
  PaperId as mag_id,
  string_agg(cast(PaperReferenceId as STRING) order by PaperReferenceId) as references
from gcp_cset_mag.PaperReferences group by PaperId