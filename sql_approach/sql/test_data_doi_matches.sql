select {TABLE1}_id, {TABLE2}_id from {DATASET}.all_4_plus_3_plus_2_plus_1
where ({TABLE1}_id is not null) and ({TABLE2}_id is not null) and
  ({TABLE1}_id in (select {TABLE1}_id from gcp_cset_links.{TABLE1}_{TABLE2}_dois))
