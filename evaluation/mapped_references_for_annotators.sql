-- retrieve article samples in bins as described here
-- https://docs.google.com/document/d/1mYAjQM4v5GldSb4x7rIu9ZbQfKHoagOHgfUViU0eaYo/edit#bookmark=id.uk69uhyj5m2p
-- excluding a bin for simhash-only matches for now (although these may appear in other bins)

CREATE TEMP FUNCTION article_desc(title STRING, abstract STRING, year INT64, doi STRING, authors STRING)
RETURNS STRING
AS (
  CONCAT("title: ", coalesce(title, ""), "\nyear: ", case when year is not null then cast(year as string) else "" end, "\ndoi: ", coalesce(doi, ""), "\nauthors: ", coalesce(authors, ""), "\nabstract: ", coalesce(abstract, ""))
);

with raw as (
  select
    id as orig_id,
    title as title,
    abstract as abstract,
    year as year,
    concat("https://doi.org/", clean_doi) as doi,
    string_agg(distinct names, ", " order by names desc) as authors,
    references
  from article_links_v3_eval.mapped_references
  cross join unnest(last_names) as names
  group by id, title, abstract, year, doi, references
)

select
  data1.orig_id as orig_id1,
  data2.orig_id as orig_id2,
  article_desc(data1.title, data1.abstract, data1.year, data1.doi, data1.authors) as metadata1,
  article_desc(data2.title, data2.abstract, data2.year, data2.doi, data2.authors) as metadata2
from raw as data1
inner join
raw as data2
using(references)
where data1.orig_id > data2.orig_id -- avoid having annotators annotate twice
limit 1000
