(select
  a.{TABLE1}_id,
  b.{TABLE2}_id,
  b.{TABLE3}_id,
from (select * from {DATASET}.all_pairs where {TABLE1}_id is not null and {TABLE2}_id is not null) a
inner join
(select * from {DATASET}.all_pairs where {TABLE2}_id is not null and {TABLE3}_id is not null) b
on a.{TABLE2}_id = b.{TABLE2}_id)