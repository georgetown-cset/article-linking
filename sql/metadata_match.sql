-- get combined set of metadata matches
select distinct all1_id, all2_id from (
{{ params.tables }}
)