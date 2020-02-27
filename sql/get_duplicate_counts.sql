select num_matches, count(num_matches) as num_occurrences from
  (select {TABLE1}1_id, count({TABLE1}2_id) as num_matches
  from test_generate_metadata.{TABLE1}_{TABLE1}_base
  group by {TABLE1}1_id)
group by num_matches order by num_occurrences desc