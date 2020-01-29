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

3.5) Get only the titles we care about:

```
create or replace table `gcp-cset-projects.wos_mag_article_linking.mag_titles` as
SELECT paperid, papertitle as title FROM `gcp-cset-projects.gcp_cset_mag.Papers` where doctype != 'Dataset' AND doctype != 'Patent'
```

cleaned_wos_metadata_20200127
