select
  PaperId, array_agg(OriginalAuthor IGNORE NULLS) as names
from gcp_cset_mag.PaperAuthorAffiliations
group by PaperId