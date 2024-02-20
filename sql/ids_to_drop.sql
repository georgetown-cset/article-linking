select distinct
    merged_id
from
    literature.sources
where
    orig_id in (select id1 from staging_literature.unlink)