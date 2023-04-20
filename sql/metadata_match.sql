-- get combined set of metadata matches
select distinct id1, id2 from (
{{ params.tables }}
)