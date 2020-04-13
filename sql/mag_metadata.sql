select
  p.PaperId as id,
  p.OriginalTitle as title,
  p.abstract,
  lower(p.Doi) as clean_doi,
  p.Year as year,
  -- these are full names, not last names, but they'll turn into last names when the cleaning script runs
  a.names as last_names,
  r.references
from gcp_cset_mag.PapersWithAbstracts p
left join
{{params.dataset}}.mag_authors a
on p.PaperId = a.PaperId
left join
  (select PaperId, string_agg(cast(PaperReferenceId as STRING) order by PaperReferenceId) as references
  from gcp_cset_mag.PaperReferences group by PaperId) r
on r.PaperId = p.PaperId
where (p.DocType != "Dataset") and (p.DocType != "Patent")
