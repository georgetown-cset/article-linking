-- get mag ids (used in validation)
select distinct(PaperId) as id from gcp_cset_mag.PapersWithAbstracts where (DocType != "Dataset") and (DocType != "Patent")
