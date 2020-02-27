select * from {DATASET}.{TABLE1}_{TABLE2}_base where
  ({TABLE1}_id in
    (select {TABLE1}_id from
      (select {TABLE1}_id, count({TABLE2}_id) as num_records from {DATASET}.{TABLE1}_{TABLE2}_base
        group by {TABLE1}_id)
  where num_records = 1))
and
  ({TABLE2}_id in
    (select {TABLE2}_id from
      (select {TABLE2}_id, count({TABLE1}_id) as num_records from {DATASET}.{TABLE1}_{TABLE2}_base
        group by {TABLE2}_id)
  where num_records = 1))
