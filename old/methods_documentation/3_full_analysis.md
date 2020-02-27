1.) `select distinct(id) from gcp_cset_clarivate.wos_dynamic_identifiers_latest`

Writing 49096840 records to `wos_dim_article_linking.all_wos_ids_20200127`

2.) Get the DOIs

```
select count(id) as num_wos_ids, identifier_value from
    (select id, identifier_value from gcp_cset_clarivate.wos_dynamic_identifiers_latest where
        (identifier_type="doi") and (identifier_value is not null))
group by identifier_value
order by num_wos_ids desc
```

Writing 24517093 records to `wos_dim_article_linking.wos_ids_with_doi_20200127`

2.5.) Filter out records that have more than one DOI.

```
select
  * from wos_dim_article_linking.wos_ids_with_doi_20200127
where identifier_value in
  (
    select identifier_value from wos_dim_article_linking.wos_ids_with_doi_20200127 where num_wos_ids = 1
  ) 
```

Writing 24438854 records to `wos_dim_article_linking.usable_wos_ids_with_doi_20200127`

2.75) Filter out DOIs that have more than one WOS id:

```
select id, identifier_value from (
  select count(id) as num_ids, max(id) as id, identifier_value from wos_dim_article_linking.usable_wos_ids_with_doi_20200127 group by identifier_value
) where num_ids = 1
```

Writing 24438854 records to `wos_dim_article_linking.really_usable_wos_ids_with_doi_20200127`

3.) Get the abstracts:

```
SELECT id, STRING_AGG(paragraph_text, "\n" ORDER BY CAST(paragraph_id AS INT64) ASC) as abstract
       FROM gcp_cset_clarivate.wos_abstract_paragraphs_latest
       WHERE abstract_id = "1"
       GROUP BY id;
```

Writing 35625094 records to `wos_dim_article_linking.wos_abstract_paragraphs_20200127`

3.5) Get only the titles we care about:

```
-- there will be only one row that satisfies min(title_id) within each id, so the MIN(title) is just there to pacify sql
select id, MIN(title_id) as title_id, MIN(title) as title from gcp_cset_clarivate.wos_titles_latest where title_type="item" group by id
```

writing XXX records to `wos_dim_article_linking.only_usable_titles_20200127` 

3.75) There are a handful of ids in wos_summary_latest with more than one record - look into this later but for now
get the min year.

```
select id, min(pubyear) as year from gcp_cset_clarivate.wos_summary_latest group by id
```

writing 49096840 records to `wos_dim_article_linking.unique_pubyears_20200127`

4.) Join everything together into one table of happiness:

```
SELECT
  DISTINCT ids.id,
  a.year,
  b.title,
  c.abstract AS abstract,
  d.identifier_value AS doi
FROM
  wos_dim_article_linking.all_wos_ids_20200127 ids
LEFT JOIN
  wos_dim_article_linking.unique_pubyears_20200127 a
ON
  ids.id = a.id
LEFT JOIN
  wos_dim_article_linking.only_usable_titles_20200127 b
ON
  ids.id = b.id
LEFT JOIN
  wos_dim_article_linking.wos_abstract_paragraphs_20200127 c
ON
  ids.id = c.id
LEFT JOIN
  wos_dim_article_linking.really_usable_wos_ids_with_doi_20200127 d
ON
  ids.id = d.id
``` 

Writing 49,097,597 records to `wos_dim_article_linking.wos_metadata_20200127`

5.) And normalize with our normalization script, into:

`wos_dim_article_linking.cleaned_wos_metadata_20200127`

6.) Also normalize all of DS, into:

`wos_dim_article_linking.cleaned_ds_20200127`

7.) Now, let's start matching. DOIs first:

DOI matches:

```
select w.id as wos_id, d.id as dim_id, w.doi from
wos_dim_article_linking.cleaned_wos_metadata_20200127 w
inner join
wos_dim_article_linking.cleaned_ds_20200127 d
on (lower(w.doi) = lower(d.doi)) and (w.doi is not null)
``` 

writing 24059104 rows to `wos_dim_article_links.doi_matches_20200128`
(the lower is important, without it only 19581087 are matched!)

Rest (wos):

```
select * from wos_dim_article_linking.cleaned_wos_metadata_20200127
where id not in (select wos_id from wos_dim_article_links.doi_matches_20200128)
```

writing 25068581 rows to `wos_dim_article_links.no_doi_match_wos`

Rest (dimensions):

```
select * from wos_dim_article_linking.cleaned_ds_20200127
where id not in (select dim_id from wos_dim_article_links.doi_matches_20200128)
```

writing 83382940 rows to `wos_dim_article_links.no_doi_match_ds`

8.) Let's identify the set of papers that have year + title + abstract matches (title not none and abstract not none):

Matches

```
select w.id as wos_id, d.id as dim_id, d.doi as dim_doi, w.doi as wos_doi, w.year as year, w.title as title, w.abstract as abstract
from wos_dim_article_links.no_doi_match_wos w
inner join
wos_dim_article_links.no_doi_match_ds d
on (w.year = d.year) and (w.year is not null) and 
   (w.title = d.title) and (w.title is not null) and (w.title != "") and
   (w.abstract = d.abstract) and (w.abstract is not null) and (w.abstract != "")
```

writing 2923782 rows to `wos_dim_article_links.title_year_abstract_matches_20200128`

Eeek. That's not a lot of matches. We can see why though:

```
select count(id) from wos_dim_article_links.no_doi_match_wos where (abstract is null) or (abstract = "") 
```

returns 10641928 rows, while

```
select count(id) from wos_dim_article_links.no_doi_match_wos where (title is null) or (title = "")
```

returns 682 rows.

Let's also do a query that allows one of title, abstract, or year to not match. The query I want to do is:

```
select w.id as wos_id, d.id as dim_id, d.doi as dim_doi, w.doi as wos_doi, w.year as year, w.title as title, w.abstract as abstract
from wos_dim_article_links.no_doi_match_wos w
inner join
wos_dim_article_links.no_doi_match_ds d
on ((w.year = d.year) and (w.year is not null) and 
   (w.title = d.title) and (w.title is not null) and (w.title != "")) or
   ((w.year = d.year) and (w.year is not null) and 
   (w.abstract = d.abstract) and (w.abstract is not null) and (w.abstract != "")) or
   ((w.title = d.title) and (w.title is not null) and (w.title != "") and 
   (w.abstract = d.abstract) and (w.abstract is not null) and (w.abstract != ""))
```

But after 45 minutes that still didn't look close to completing so instead I'll split the or into three separate
queries, like this:

```
select w.id as wos_id, d.id as dim_id, d.doi as dim_doi, w.doi as wos_doi, w.year as year, w.title as title, w.abstract as abstract
from wos_dim_article_links.no_doi_match_wos w
inner join
wos_dim_article_links.no_doi_match_ds d
on ((w.year = d.year) and (w.year is not null) and 
   (w.title = d.title) and (w.title is not null) and (w.title != ""))
```

writing 46,065,454 rows to `wos_dim_article_links.year_title_pairwise_match_pre_filter`
writing 3,136,664 rows to `wos_dim_article_links.year_abstract_match_pre_filter`
writing 2,993,898 rows to `wos_dim_article_links.title_abstract_match_pre_filter`

Next, let's union these and get the distinct rows.

```
select distinct(*) from wos_dim_article_links.year_title_match_pre_filter
union all
wos_dim_article_links.year_abstract_match_pre_filter
union all
wos_dim_article_links.title_abstract_match_pre_filter
```

writing 46348452 rows to `wos_dim_article_links.year_title_abstract_one_pairwise_match_pre_filter`

The output numbers look nice, but in reality the pairs matched by the query above may contain ids that are present
in other matches, inflating the count. So let's now filter those results to only wos and dimensions pairs where the
elements of each pair occur exactly once in the output table.

```
select * from wos_dim_article_links.year_title_abstract_one_pairwise_match_pre_filter
where 
(wos_id in (
    select wos_id from (
        select wos_id, count(wos_id) as num_appearances from wos_dim_article_links.year_title_abstract_one_pairwise_match_pre_filter
        group by wos_id
    ) where num_appearances = 1
))
and
    (ds_id in (
    select ds_id from (
        select ds_id, count(ds_id) as num_appearances from ds_dim_article_links.year_title_abstract_one_pairwise_match_pre_filter
        group by ds_id
    ) where num_appearances = 1
))
```

writing 8,276,025 rows to `wos_dim_article_links.year_title_abstract_one_pairwise_match`

At this point, we have successfully matched 8276025+24059104 = 32,335,129 WOS ids, leaving 16,761,711 remaining to
match. We'll now use our text similarity script.

9.) Let's get the unmatched records:

```
select * from wos_dim_article_linking.wos_metadata_20200127 where
(id not in (select wos_id from wos_dim_article_links.year_title_abstract_one_pairwise_match)) and
(id not in (select wos_id from wos_dim_article_links.doi_matches_20200128))
```

16,792,556 records in `wos_dim_article_links.unmatched_wos_ids`, of which roughly half (8,832,307) have null abstracts

10.) Non-exact matching on title alone seems dangerous, even within year. We have some records like the three returned
by

```
select * from wos_dim_article_links.unmatched_wos_ids
where title="clinical characterisation of neurexin deletions and their role in neurodevelopmental disorders"
``` 

that have no information in our metadata table other than their (identical) titles and years, and their (different) ids

