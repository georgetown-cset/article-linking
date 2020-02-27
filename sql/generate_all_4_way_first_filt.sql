select arxiv_id, min(wos_id) as wos_id, min(ds_id) as ds_id, min(mag_id) as mag_id
from {DATASET}.all_4_way_first group by arxiv_id