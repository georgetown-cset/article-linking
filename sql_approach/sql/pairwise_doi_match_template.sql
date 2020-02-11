select a.id as {TABLE1}_id, w.id as {TABLE2}_id
from {DATASET}.{TABLE1}_dois a inner join {DATASET}.{TABLE2}_dois w on a.clean_doi = w.clean_doi