1.) DOI can't necessarily be used to construct the set of trivially linkable articles. For example, any of these
dois with > 1 WOS id:

```
select count(id) as num_wos_ids, identifier_value from
    (select id, identifier_value from gcp_cset_clarivate.wos_dynamic_identifiers_latest where
        (identifier_type="doi") and (identifier_value is not null))
group by identifier_value
order by num_wos_ids desc
```

[10.1097/01.PRS.0000141485.83476.89](http://apps.webofknowledge.com/Search.do?product=WOS&SID=7AesKAwMb5jwfX4DSGq&search_mode=GeneralSearch&prID=3b743f6d-2e2c-4a94-8300-e529a1680c27)

Maybe it's some combination of page range and publication??

[10.1111/j.1365-2435.2007.01258.x](http://apps.webofknowledge.com/Search.do?product=WOS&SID=7AesKAwMb5jwfX4DSGq&search_mode=GeneralSearch&prID=e3349b8b-a5f6-4187-b7f3-8cf5c2dd62d6)

writing to: `gcp-cset-projects:wos_dim_article_linking.wos_id_counts_by_doi_20200108`

with 24,406,130 rows

2.) Grab the IDs with only one DOI:

```
select
  id, identifier_value from gcp_cset_clarivate.wos_dynamic_identifiers_latest 
where 
  (identifier_value in
    (
      select identifier_value from wos_dim_article_linking.wos_id_counts_by_doi_20200108
      where num_wos_ids = 1
    )
  ) and (identifier_type="doi") 
```

writing to: `gcp-cset-projects:wos_dim_article_linking.usable_wos_doi_ids_20200109`

Ok: 24329498 rows as expected (as expected because we're throwing out rows that reference non-unique DOIs, so we
should have fewer than we had in (1)).

2.5.) Clarivate provides abstract paragraphs separately, one per row of wos_abstract_paragraphs_latest. There are
also multiple versions of the abstracts. We're going to just take the first version, and then use the ordering of the
paragraph_id to construct the full abstract.

```
SELECT id, STRING_AGG(paragraph_text, "\n" ORDER BY CAST(paragraph_id AS INT64) ASC) as abstract
       FROM gcp_cset_clarivate.wos_abstract_paragraphs_latest
       WHERE abstract_id = "1"
       GROUP BY id;
```

written to: `wos_dim_article_linking.concat_wos_abstract_paragraphs_20190109`

3.) Let's add some metadata onto these rows, and remove RSCI and CSCD! Thanks to Daniel this is easy, we can use a
modified version of his `cset_wos_corpus` query. 

```
SELECT
  id,
  pubyear,
  title,
  abstract,
  doi
FROM (
  SELECT
    DISTINCT a.id,
    a.pubyear,
    b.title,
    c.abstract AS abstract,
    d.identifier_value AS doi
  FROM
    gcp_cset_clarivate.wos_summary_latest a
  INNER JOIN
    gcp_cset_clarivate.wos_titles_latest b
  ON
    a.id = b.id
    AND b.title_type = 'item'
  INNER JOIN
    wos_dim_article_linking.concat_wos_abstract_paragraphs_20190109 c
  ON
    a.id = c.id
  INNER JOIN
    wos_dim_article_linking.usable_wos_doi_ids_20200109 d
  ON
    a.id = d.id
  WHERE (a.id LIKE "WOS%")
)
```

writing to: `gcp-cset-projects:wos_dim_article_linking.usable_wos_doi_rows_20200109`

We now have 21,353,190 rows. In checking for uniqueness of ids here, however, I discovered that one WOS ID can
also correspond to multiple DOIs! James pointed out that these seem to be revisions of the same base paper.

`select * from wos_dim_article_linking.usable_wos_doi_rows_20200109 where id="WOS:000500997800023"`

Let's deal with this later, and press onward for now.

4.) On to Digital Science. We want to figure out where we can get doi, pubyear, title, and abstract. It's so easy...

```
SELECT id, doi, year as pubyear, title, abstract
FROM gcp_cset_digital_science.dimensions_publications_with_abstracts_latest
WHERE doi is not null
```

writing to: `gcp-cset-projects:wos_dim_article_linking.dimensions_with_doi_20200109`

We have 97014156 rows. But let's double-check uniqueness of doi and dimensions id. As we expect, given this is a _latest
table, we have unique dimensions ids, but as we saw in Clarivate, DOIs are not unique.

select count(distinct(id)) from wos_dim_article_linking.dimensions_with_doi_20200109
> 97014156

select count(distinct(doi)) from wos_dim_article_linking.dimensions_with_doi_20200109
> 96904976

5.) So let's get rid of the rows with non-unique DOIs like we did for Clarivate.

```
SELECT * FROM wos_dim_article_linking.dimensions_with_doi_20200109
WHERE doi IN (
  SELECT doi from (
    SELECT count(id) as num_dim_ids, doi
    FROM wos_dim_article_linking.dimensions_with_doi_20200109
    GROUP BY doi
  )
  WHERE num_dim_ids = 1
)
```

writing to: `wos_dim_article_linking.usable_dimensions_doi_rows_20190109`

with 96,799,485 rows as expected (that is, we expect the number to be less than the number of unique DOIs, 
because we're filtering out the rows with non-unique DOIs.

6.) Ok, time to glue it all together!

```
SELECT
  d.doi as doi,
  d.id as ds_id,
  w.id as wos_id,
  d.pubyear as ds_year,
  CAST(w.pubyear AS INT64) as wos_year,
  d.title as ds_title,
  w.title as wos_title,
  d.abstract as ds_abstract,
  w.abstract as wos_abstract
FROM
  wos_dim_article_linking.usable_dimensions_doi_rows_20190109 d
INNER JOIN
  wos_dim_article_linking.usable_wos_doi_rows_20200109 w
ON
  d.doi = w.doi
```

writing to:

`wos_dim_article_linking.doi_match_wos_dimensions_20200109`

With 17,196,623 rows.

7.) We now need to make a decision about handling multiple versions of the same paper with different DOIs as
noted in (4), and WOS IDs that correspond to multiple dimensions IDs. For now, as I don't expect this to make
a big difference (there are:
 
- 17,196,573 unique WOS IDs
- 17,196,578 unique Dimensions IDs
- 17,196,578 unique DOIs
- 17,196,623 rows

in (6). But just to make the eval set as possible, let's clean these up as well, as well as the 134218 (!) records
where year doesn't match (these seem to be due, at least in part, to differences between publication date in 
an English-speaking and a foreign-language journal):

```
select * from wos_dim_article_linking.doi_match_wos_dimensions_20200109 where (wos_year = ds_year) and (wos_id in (
  select wos_id from (
    select count(ds_id) as num_ds_ids, wos_id from wos_dim_article_linking.doi_match_wos_dimensions_20200109 group by wos_id
  ) where num_ds_ids = 1
))
```

writing to: `wos_dim_article_linking.filtered_doi_match`

and leaving us with 17,062,190 rows.

8.) Oops I forgot authors. Let's construct tables of last names per id and then add those last names to our
match table.

```
select
  id, array_agg(last_name IGNORE NULLS) as author_last_names
from gcp_cset_clarivate.wos_summary_names_latest
where role="author"
group by id
```

writing to `wos_dim_article_linking.wos_author_last_names_2020013`

```
select
  id,
  ARRAY(select last_name from UNNEST(author_affiliations)) as last_name
from gcp_cset_digital_science.dimensions_publications
```

writing to `wos_dim_article_linking.dimensions_author_last_names_2020013`

and finally

```
SELECT
  j.doi,
  j.ds_id,
  j.wos_id,
  j.ds_year,
  j.wos_year,
  j.ds_title,
  j.wos_title,
  j.ds_abstract,
  j.wos_abstract,
  d.last_name as ds_last_names,
  w.author_last_names as wos_last_names
FROM
  wos_dim_article_linking.filtered_doi_match j
INNER JOIN
  wos_dim_article_linking.wos_author_last_names_2020013 w
ON
  j.wos_id = w.id
INNER JOIN
  wos_dim_article_linking.dimensions_author_last_names_2020013 d
ON
  j.ds_id = d.id
```

writing to `wos_dim_article_linking.filtered_doi_match_with_authors`

8.) Let's do some basic preprocessing now, to:

- strip punctuation
- downcase
- normalize whitespace (convert all spaces and newlines to ' ')
- nfkc text normalization

Using `utils/clean_corpus.py`

And put the output in :

`wos_dim_article_linking.clean_filtered_doi_match`

Other possible preprocessing can come in linkage method-specific code.

Some things to note:

- number of empty titles in DS:
- number of empty titles in WOS:
- number of empty abstracts in DS:
- number of empty abstracts in WOS:
- number of mismatched author lists:

We'll resume this thrilling narrative in `partitioning.md`. :)