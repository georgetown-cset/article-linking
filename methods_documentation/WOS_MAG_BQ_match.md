.) Get MAG IDs
```
create or replace table `gcp-cset-projects.wos_mag_article_linking.mag_id` as
select distinct PaperID, Year from `gcp-cset-projects.gcp_cset_mag.Papers` where doctype != 'Dataset' AND doctype != 'Patent'
```
 Writing 179,043,616 records to `wos_mag_article_linking.mag_id`

2.) Get the MAG DOIs

```
create or replace table `gcp-cset-projects.wos_mag_article_linking.mag_ids_with_doi` as
select * except(doi1) from (SELECT
  COUNT(PaperID) AS num_mag_ids,
  doi
FROM
  `gcp-cset-projects.gcp_cset_mag.Papers` where doctype != 'Dataset' AND doctype != 'Patent'
GROUP BY
  doi) dois inner join (select paperid, doi as doi1 from `gcp-cset-projects.gcp_cset_mag.Papers`) as ids
  on dois.doi = ids.doi1
```
Writing 84,077,577 records to `gcp-cset-projects.wos_mag_article_linking.usable_mag_ids_with_doi`

2.5.) Filter out records that have more than one DOI.

```
create or replace table `gcp-cset-projects.wos_mag_article_linking.usable_mag_ids_with_doi` as
select
  doi, paperid from `gcp-cset-projects.wos_mag_article_linking.mag_ids_with_doi`
where  doi in
  (
    select doi from  `gcp-cset-projects.wos_mag_article_linking.mag_ids_with_doi` where num_mag_ids = 1
  ) 
```

Writing 83,720,552 records to `wos_mag_article_linking.usable_mag_ids_with_do`

2.75) Filter out DOIs that have more than one WOS id:

```
create or replace table `gcp-cset-projects.wos_mag_article_linking.really_usable_mag_ids_with_doi` as
select paperid, doi from (
  select count(paperid) as num_ids, max(paperid) as paperid, doi from `gcp-cset-projects.wos_mag_article_linking.usable_mag_ids_with_doi` group by doi
) where num_ids = 1
```

Writing 83,703,516 records to `gcp-cset-projects.wos_mag_article_linking.really_usable_mag_ids_with_doi` 

3.) Get the abstracts:

```
create or replace table `gcp-cset-projects.wos_mag_article_linking.mag_abstracts` as SELECT paperid, norm_abstract as abstract FROM `gcp-cset-projects.gcp_cset_mag.PaperAbstracts` where paperid in (select paperid from `gcp-cset-projects.gcp_cset_mag.Papers` where doctype != 'Dataset' AND doctype != 'Patent')
```

Writing 91,827,296 records to `gcp-cset-projects.wos_mag_article_linking.mag_abstracts`

3.5) Get only the s we care about:

```
create or replace table `gcp-cset-projects.wos_mag_article_linking.mag_titles` as
SELECT paperid, OriginalTitle as title FROM `gcp-cset-projects.gcp_cset_mag.PapersWithCleanTitles` where paperid in ( select paperid from `gcp-cset-projects.gcp_cset_mag.Papers` where doctype != 'Dataset' AND doctype != 'Patent')
```

cleaned_wos_metadata_20200127



3.75) There are not duplicated IDs in mag

4.) Join everything together into one table of happiness:

```
create or replace table `gcp-cset-projects.wos_mag_article_linking.mag_metadata` as
SELECT
  DISTINCT ids.Paperid,
  ids.year,
  b.title,
  c.abstract AS abstract,
  d.doi
FROM
  `gcp-cset-projects.wos_mag_article_linking.mag_id` ids
LEFT JOIN
  `gcp-cset-projects.wos_mag_article_linking.mag_titles` b
ON
  ids.Paperid = b.Paperid
LEFT JOIN
  `gcp-cset-projects.wos_mag_article_linking.mag_abstracts` c
ON
  ids.Paperid = c.Paperid
LEFT JOIN
  `gcp-cset-projects.wos_mag_article_linking.really_usable_mag_ids_with_doi` d
ON
  ids.Paperid = d.Paperid
``` 

Writing 179,043,616 records to `gcp-cset-projects.wos_mag_article_linking.mag_metadata`

5.) And normalize with our normalization script, into:

`wos_dim_article_linking.cleaned_wos_metadata_20200127`

6.) Also normalize all of DS, into:

`wos_dim_article_linking.cleaned_ds_20200127`

7.) Now, let's start matching. DOIs first:

DOI matches:

```
create or replace table `gcp-cset-projects.wos_mag_article_linking.doi_matches` as
select m.paperid as mag_id, d.id as wos_id, m.doi from
`gcp-cset-projects.wos_mag_article_linking.mag_metadata` m
inner join
`wos_dim_article_linking.cleaned_wos_metadata_20200127` d
on (lower(m.doi) = lower(d.doi)) and (m.doi is not null)
``` 

writing 23,455,443 rows to `gcp-cset-projects.wos_mag_article_linking.doi_matches`

Rest (wos):

```
create or replace table `gcp-cset-projects.wos_mag_article_linking.no_doi_match_mag` as
select * from `gcp-cset-projects.wos_mag_article_linking.mag_metadata`
where paperid not in (select mag_id from `gcp-cset-projects.wos_mag_article_linking.doi_matches`)
```

writing 155,588,356 rows to `gcp-cset-projects.wos_mag_article_linking.no_doi_match_mag`

Rest (WOS):

```
create or replace table `gcp-cset-projects.wos_mag_article_linking.no_doi_match_wos` as
select * from wos_dim_article_linking.cleaned_wos_metadata_20200127
where id not in (select wos_id from `gcp-cset-projects.wos_mag_article_linking.doi_matches`)
```

writing 25,643,555 rows to `gcp-cset-projects.wos_mag_article_linking.no_doi_match_ds`

8.) Let's identify the set of papers that have year + title + abstract matches (title not none and abstract not none):

Matches

```
create or replace table `gcp-cset-projects.wos_mag_article_linking.title_year_abstract_matches` as
select m.paperid as mag_id, d.id as wos_id, d.doi as wos_doi, m.doi as mag_doi, CAST(m.year as int64) as year, m.title as title, m.abstract as abstract
from `gcp-cset-projects.wos_mag_article_linking.no_doi_match_mag` m
inner join
`gcp-cset-projects.wos_mag_article_linking.no_doi_match_wos` d
on (CAST(m.year as int64) = d.year) and (m.year is not null) and 
   (m.title = d.title) and (m.title is not null) and (m.title != "") and
   (m.abstract = d.abstract) and (m.abstract is not null) and (m.abstract != "")
```

writing 2,879,923 rows to `wos_mag_article_linking.title_year_abstract_matches`

Eeek. That's not a lot of matches. We can see why though:

```
select count(paperid) from `gcp-cset-projects.wos_mag_article_linking.no_doi_match_mag` where (abstract is null) or (abstract = "") 
```

returns 84,004,840 rows, while

```
select count(paperid) from `gcp-cset-projects.wos_mag_article_linking.no_doi_match_mag` where (title is null) or (title = "")
```

returns 17 rows.

Let's also do a query that allows one of title, abstract, or year to not match. The query I want to do is:


```
create or replace table `gcp-cset-projects.wos_mag_article_linking.year_title_pairwise_match_pre_filter` as select m.paperid as mag_id, d.id as wos_id, d.doi as wos_doi, m.doi as mag_doi, CAST(m.year as int64) as year, m.title as title, m.abstract as abstract
from `gcp-cset-projects.wos_mag_article_linking.no_doi_match_mag`  m
inner join
`gcp-cset-projects.wos_mag_article_linking.no_doi_match_wos` d
on (CAST(m.year as int64) = d.year) and (m.year is not null) and 
   (m.title = d.title) and (m.title is not null) and (m.title != "")
```  

writing 13,128,726 rows to `wos_wos_article_links.year_title_pairwise_match_pre_filter`

```
create or replace table `gcp-cset-projects.wos_mag_article_linking.year_abstract_match_pre_filter` as select m.paperid as mag_id, d.id as wos_id, d.doi as wos_doi, m.doi as mag_doi, CAST(m.year as int64) as year, m.title as title, m.abstract as abstract
from `gcp-cset-projects.wos_mag_article_linking.no_doi_match_mag`  m
inner join
`gcp-cset-projects.wos_mag_article_linking.no_doi_match_wos` d
on (CAST(m.year as int64) = d.year)  and (CAST(m.year as int64) is not null) and 
   (m.abstract = d.abstract) and (m.abstract is not null) and (m.abstract != "")
````

writing 3,184,655 rows to `wos_wos_article_links.year_abstract_match_pre_filter`

```
create or replace table `gcp-cset-projects.wos_mag_article_linking.title_abstract_match_pre_filter` as select m.paperid as mag_id, d.id as wos_id, d.doi as wos_doi, m.doi as mag_doi, CAST(m.year as int64) as year, m.title as title, m.abstract as abstract
from `gcp-cset-projects.wos_mag_article_linking.no_doi_match_mag`  m
inner join
`gcp-cset-projects.wos_mag_article_linking.no_doi_match_wos` d
on (m.title = d.title) and (m.title is not null) and (m.title != "") and 
   (m.abstract = d.abstract) and (m.abstract is not null) and (m.abstract != "")
```

writing 2,990,277 rows to `wos_wos_article_links.title_abstract_match_pre_filter`

Next, let's union these and get the distinct rows.

```
create or replace table `gcp-cset-projects.wos_mag_article_linking.year_title_abstract_one_pairwise_match_pre_filter` as
select distinct *  from `gcp-cset-projects.wos_mag_article_linking.year_title_pairwise_match_pre_filter`
union all
(select * from `gcp-cset-projects.wos_mag_article_linking.year_abstract_match_pre_filter`)
union all
(select * from `gcp-cset-projects.wos_mag_article_linking.title_abstract_match_pre_filter`) 
```

writing 19,303,658 rows to `gcp-cset-projects.wos_mag_article_linking.year_title_abstract_one_pairwise_match_pre_filter`

The output numbers look nice, but in reality the pairs matched by the query above may contain ids that are present
in other matches, inflating the count. So let's now filter those results to only mag and WOS pairs where the
elements of each pair occur exactly once in the output table.

```
create or replace table `gcp-cset-projects.wos_mag_article_linking.year_title_abstract_one_pairwise_match` as
select * from `gcp-cset-projects.wos_mag_article_linking.year_title_abstract_one_pairwise_match_pre_filter`
where 
(mag_id in (
    select mag_id from (
        select mag_id, count(mag_id) as num_appearances from `gcp-cset-projects.wos_mag_article_linking.year_title_abstract_one_pairwise_match_pre_filter`
        group by mag_id
    ) where num_appearances = 1
))
and
    (wos_id in (
    select wos_id from (
        select wos_id, count(wos_id) as num_appearances from `gcp-cset-projects.wos_mag_article_linking.year_title_abstract_one_pairwise_match_pre_filter`
        group by wos_id
    ) where num_appearances = 1
))
```

writing 8,771,601 rows to `gcp-cset-projects.wos_mag_article_linking.year_title_abstract_one_pairwise_match`

At this point, we have successfully matched 8,771,601 + 23,455,443 = 32,227,044 ids, leaving 146,816,572/179,043,616= MAG IDs AND 16,871,954/49,097,580 DS IDs remaining to
match. We'll now use our text similarity script.

9.) Let's get the unmatched records:

```
create or replace table `gcp-cset-projects.wos_mag_article_linking.unmatched_mag_ids` as
select * from `gcp-cset-projects.wos_mag_article_linking.mag_metadata` where
(paperid not in (select mag_id from `gcp-cset-projects.wos_mag_article_linking.year_title_abstract_one_pairwise_match`)) and
(paperid not in (select mag_id from `gcp-cset-projects.wos_mag_article_linking.doi_matches`))
```
Count number of missing abstracts in the unmatched MAG records
```
select count(*) from `gcp-cset-projects.wos_mag_article_linking.mag_metadata` where paperid in (select paperid from  `gcp-cset-projects.wos_mag_article_linking.unmatched_mag_ids`) and (abstract = ""  or abstract is null)
```
146,816,755 records in `wos_wos_article_links.unmatched_mag_ids`, of which roughly half (81,493,488) have null abstracts

```
select * from (select count(title) as title_ct, paperid from `gcp-cset-projects.wos_mag_article_linking.unmatched_mag_ids` group by paperid) where title_ct > 1
```

10.) In the unmatched MAG ids the titles are unique, Yay!
Get the list of DOIs in Mag References:
MAG
```
create or replace table `gcp-cset-projects.wos_mag_article_linking.mag_ref_dois` as
select paperid, ARRAY_CONCAT_AGG([doi] ORDER BY paperid) AS ref_doi_list
from  (select * except(id1)  from (select * from `gcp-cset-projects.gcp_cset_mag.PaperReferences`) ref inner join (select paperid as id1, LOWER(doi) as doi from `gcp-cset-projects.gcp_cset_mag.Papers` where doi != '' and doi is not null) dois on ref.PaperReferenceId = dois.id1) group by paperid
```
WOS
```
create or replace table `gcp-cset-projects.wos_mag_article_linking.wos_ref_dois` as
select id as wos_id, ref_id, doi from `gcp-cset-projects.gcp_cset_clarivate.wos_references_latest`
where doi != '' and doi is not null 
```




