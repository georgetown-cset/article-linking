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
  * from wos_dim_article_linking.usable_wos_ids_with_doi_20200127 
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



6.) Also normalize all of DS, into:

`wos_dim_article_linking.cleaned_ds_20200127`



