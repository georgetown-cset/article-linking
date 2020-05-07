select distinct(PaperId) as id from gcp_cset_mag.PapersWithAbstracts where (DocType != "Dataset") and (DocType != "Patent")
