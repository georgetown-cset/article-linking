select
  p.PaperId as id,
  p.Year as year,
  p.abstract,
  p.OriginalTitle as title,
  -- these are full names, not last names, but they'll turn into last names when the cleaning script runs
  a.names as last_names
from gcp_cset_mag.PapersWithAbstracts p
left join
{DATASET}.mag_authors a
on p.PaperId = a.PaperId
where (p.DocType != "Dataset") and (p.DocType != "Patent")