-- retrieve article samples in bins as described here
-- https://docs.google.com/document/d/1mYAjQM4v5GldSb4x7rIu9ZbQfKHoagOHgfUViU0eaYo/edit#bookmark=id.uk69uhyj5m2p
-- excluding a bin for simhash-only matches for now (although these may appear in other bins)

CREATE TEMP FUNCTION article_desc(title STRING, abstract STRING, year INT64, doi STRING, authors STRING)
RETURNS STRING
AS (
  CONCAT("title: ", coalesce(title, ""), "\nyear: ", case when year is not null then cast(year as string) else "" end, "\ndoi: ", coalesce(doi, ""), "\nauthors: ", coalesce(authors, ""), "\nabstract: ", coalesce(abstract, ""))
);

with id_counts as (
  select merged_id, count(distinct(orig_id)) as num_matches from gcp_cset_links_v2.article_links group by merged_id order by rand()
),

match_band_sample as (
  (select merged_id, "count_2" as bin from id_counts where num_matches = 2 limit 25)
  union all
  (select merged_id, "count_3" as bin from id_counts where num_matches = 3 limit 25)
  union all
  (select merged_id, "count_4" as bin from id_counts where num_matches = 4 limit 25)
  union all
  (select merged_id, "count_5" as bin from id_counts where num_matches = 5 limit 25)
  union all
  (select merged_id, "count_6" as bin from id_counts where num_matches = 6 limit 5)
  union all
  (select merged_id, "count_7" as bin from id_counts where num_matches = 7 limit 5)
  union all
  (select merged_id, "count_8" as bin from id_counts where num_matches = 8 limit 5)
  union all
  (select merged_id, "count_9" as bin from id_counts where num_matches = 9 limit 5)
  union all
  (select merged_id, "count_10" as bin from id_counts where num_matches = 10 limit 5)
  union all
  (select merged_id, "count_11_15" as bin from id_counts where num_matches > 10 and num_matches < 16 limit 50)
),

citation_counts as (
  select ref_id as merged_id, count(distinct(merged_id)) as num_citations from gcp_cset_links_v2.paper_references_merged group by ref_id
),

top_cited_sample as (
  select merged_id, "high_citation" as bin from citation_counts where merged_id not in (select merged_id from match_band_sample) order by num_citations desc limit 50
),

full_sample as (
  (select merged_id, bin from match_band_sample)
  union all
  (select merged_id, bin from top_cited_sample)
),

raw as (
  select
    max(merged_id) as merged_id,
    max(bin) as bin,
    orig_id,
    max(title) as title,
    max(abstract) as abstract,
    max(year) as year,
    max(concat("https://doi.org/", clean_doi)) as doi,
    string_agg(distinct names, ", " order by names desc) as authors
  from full_sample
  inner join
  gcp_cset_links_v2.article_links
  using(merged_id)
  left join
  gcp_cset_links_v2.all_metadata_with_cld2_lid
  on all_metadata_with_cld2_lid.id = article_links.orig_id
  cross join unnest(last_names) as names
  group by orig_id
)

select
  merged_id,
  bin,
  orig_id,
  article_desc(title, abstract, year, doi, authors) as metadata
from raw
