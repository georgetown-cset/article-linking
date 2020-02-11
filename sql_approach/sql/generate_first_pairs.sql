select {TABLE1}_id, min({TABLE2}_id) as {TABLE2}_id from {DATASET}.{TABLE1}_{TABLE2}_base group by {TABLE1}_id
