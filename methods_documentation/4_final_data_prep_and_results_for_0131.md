## The Plan

After refactoring our approach into `in_memory_match.py`, we have an approach that on our small 50K WOS, 100K DS
evaluation set (WOS in `wos_dim_article_linking.50K_nontrivial_sample` and DS in
`wos_dim_article_linking.50K_plus_50K_eval_set`) has a precision of 1.0, recall of 0.976, and f1 of 0.988 at a 
threshold of 0.6. It's time to see what happens when we run it on the full datasets.

Having done the work of making "simple" matches in (`3_full_analysis.md`, `DS_mag_BQ_match.md`, and
`WOS_MAG_BQ_match.md`), we can now use these as a large evaluation set to get a sense of whether these performance
numbers hold up when our approach is applied to the full datasets.

We will now try to do the following matches: 

- arXiv - WOS
- WOS - DS
- arXiv - MAG
- WOS - MAG
- DS - MAG

## Data Preparation

#### arXiv

- Get authors (`wos_dim.arxiv_authors`)

```
select
  id,
  ARRAY(select keyname from UNNEST(authors.author)) as last_name
from gcp_cset_arxiv_metadata.arxiv_metadata_latest
```

- Get metadata (`wos_dim.arxiv_metadata`)

```
select
  p.id,
  p.title,
  p.abstract,
  extract(year from p.created) as year,
  a.last_name
from gcp_cset_arxiv_metadata.arxiv_metadata_latest p
left join
wos_dim.arxiv_authors a
on a.id = p.id
```

#### WOS

- Get authors (`wos_dim.wos_authors`)

```

```

- Create metadata table (`wos_dim.wos_metadata`)

```

```

#### DS

- Get authors (`wos_dim.ds_authors`)

```

```

- Create metadata table (`wos_dim.ds_metadata`)

```

```

#### MAG

- Get authors (`wos_dim.mag_authors`)

```
select
  PaperId, array_agg(OriginalAuthor IGNORE NULLS) as names
from gcp_cset_mag.PaperAuthorAffiliations
group by PaperId
```

- Create metadata table (`wos_dim.mag_metadata`)

```
select
  p.PaperId as id,
  p.Year as year,
  p.abstract,
  p.OriginalTitle as title,
  a.names as last_names -- these are not really last names
from gcp_cset_mag.PapersWithAbstracts p
left join
wos_dim.mag_authors a
on p.PaperId = a.PaperId
where (p.DocType != "Dataset") and (p.DocType != "Patent")
```

### Intermediate SQL-only solution

After creating `_clean` versions of the above metadata tables using AggressiveScrub, we can do a "simple"
SQL-only match while we wait on the matcher to run. The following sequence of queries show how the
arxiv-wos-ds-mag table was created, and we see (at the end) some reporting on performance wrt the 1-to-1 DOI matches.

#### Title-Year matches

`arxiv_mag_title_year`

```
select a.id as arxiv_id, m.id as mag_id
from wos_dim.arxiv_metadata_clean a
inner join
wos_dim.mag_metadata_clean m
on ((a.year = m.year) and (a.year is not null) and 
   (a.title_norm = m.title_norm) and (m.title_norm is not null) and (a.title_norm != ""))
```

--

`ds_mag_title_year`

```
select a.id as ds_id, m.id as mag_id
from wos_dim.ds_metadata_clean a
inner join
wos_dim.mag_metadata_clean m
on ((a.year = m.year) and (a.year is not null) and 
   (a.title_norm = m.title_norm) and (m.title_norm is not null) and (a.title_norm != ""))
```

--

`wos_mag_title_year`

```
select a.id as wos_id, m.id as mag_id
from wos_dim.wos_metadata_clean a
inner join
wos_dim.mag_metadata_clean m
on ((a.year = m.year) and (a.year is not null) and 
   (a.title_norm = m.title_norm) and (m.title_norm is not null) and (a.title_norm != ""))
```

--

`arxiv_ds_title_year`

```
select a.id as arxiv_id, m.id as ds_id
from wos_dim.arxiv_metadata_clean a
inner join
wos_dim.ds_metadata_clean m
on ((a.year = m.year) and (a.year is not null) and 
   (a.title_norm = m.title_norm) and (m.title_norm is not null) and (a.title_norm != ""))
```

--

`arxiv_wos_title_year`

```
select a.id as arxiv_id, m.id as wos_id
from wos_dim.arxiv_metadata_clean a
inner join
wos_dim.wos_metadata_clean m
on ((a.year = m.year) and (a.year is not null) and 
   (a.title_norm = m.title_norm) and (m.title_norm is not null) and (a.title_norm != ""))
```

--

`wos_ds_title_year`

```
select a.id as wos_id, m.id as ds_id
from wos_dim.wos_metadata_clean a
inner join
wos_dim.ds_metadata_clean m
on ((a.year = m.year) and (a.year is not null) and 
   (a.title_norm = m.title_norm) and (m.title_norm is not null) and (a.title_norm != ""))
```

#### Abstract-Year matches

`arxiv_mag_abstract_year`

```
select a.id as arxiv_id, m.id as mag_id
from wos_dim.arxiv_metadata_clean a
inner join
wos_dim.mag_metadata_clean m
on ((a.year = m.year) and (a.year is not null) and 
   (a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and (m.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != ""))
```

--

`ds_mag_abstract_year`

```
select a.id as ds_id, m.id as mag_id
from wos_dim.ds_metadata_clean a
inner join
wos_dim.mag_metadata_clean m
on ((a.year = m.year) and (a.year is not null) and 
   (a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and (m.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != ""))
```

--

`wos_mag_abstract_year`

```
select a.id as wos_id, m.id as mag_id
from wos_dim.wos_metadata_clean a
inner join
wos_dim.mag_metadata_clean m
on ((a.year = m.year) and (a.year is not null) and 
   (a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and (m.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != ""))
```

--

`arxiv_ds_abstract_year`

```
select a.id as arxiv_id, m.id as ds_id
from wos_dim.arxiv_metadata_clean a
inner join
wos_dim.ds_metadata_clean m
on ((a.year = m.year) and (a.year is not null) and 
   (a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and (m.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != ""))
```

--

`arxiv_wos_abstract_year`

```
select a.id as arxiv_id, m.id as wos_id
from wos_dim.arxiv_metadata_clean a
inner join
wos_dim.wos_metadata_clean m
on ((a.year = m.year) and (a.year is not null) and 
   (a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and (m.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != ""))
```

--

`wos_ds_abstract_year`

```
select a.id as wos_id, m.id as ds_id
from wos_dim.wos_metadata_clean a
inner join
wos_dim.ds_metadata_clean m
on ((a.year = m.year) and (a.year is not null) and 
   (a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and (m.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != ""))
```

#### Abstract - Title matches

`arxiv_mag_abstract_title`

```
select a.id as arxiv_id, m.id as mag_id
from wos_dim.arxiv_metadata_clean a
inner join
wos_dim.mag_metadata_clean m
on ((a.title_norm = m.title_norm) and (a.title_norm is not null) and (a.title_norm != "") and 
   (a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and (m.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != ""))
```

`ds_mag_abstract_title`

```
select a.id as ds_id, m.id as mag_id
from wos_dim.ds_metadata_clean a
inner join
wos_dim.mag_metadata_clean m
on ((a.title_norm = m.title_norm) and (a.title_norm is not null) and (a.title_norm != "") and 
   (a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and (m.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != ""))
```

`wos_mag_abstract_title`

```
select a.id as wos_id, m.id as mag_id
from wos_dim.wos_metadata_clean a
inner join
wos_dim.mag_metadata_clean m
on ((a.title_norm = m.title_norm) and (a.title_norm is not null) and (a.title_norm != "") and 
   (a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and (m.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != ""))
```

`arxiv_ds_abstract_title`

```
select a.id as arxiv_id, m.id as ds_id
from wos_dim.arxiv_metadata_clean a
inner join
wos_dim.ds_metadata_clean m
on ((a.title_norm = m.title_norm) and (a.title_norm is not null) and (a.title_norm != "") and 
   (a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and (m.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != ""))
```

`arxiv_wos_abstract_title`

```
select a.id as arxiv_id, m.id as wos_id
from wos_dim.arxiv_metadata_clean a
inner join
wos_dim.wos_metadata_clean m
on ((a.title_norm = m.title_norm) and (a.title_norm is not null) and (a.title_norm != "") and 
   (a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and (m.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != ""))
```

`wos_ds_abstract_title`

```
select a.id as wos_id, m.id as ds_id
from wos_dim.wos_metadata_clean a
inner join
wos_dim.ds_metadata_clean m
on ((a.title_norm = m.title_norm) and (a.title_norm is not null) and (a.title_norm != "") and 
   (a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and (m.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != ""))
```

#### Title-Author matches

`arxiv_mag_names_title`

```
select a.id as arxiv_id, m.id as mag_id
from wos_dim.arxiv_metadata_clean a
inner join
wos_dim.mag_metadata_clean m
on ((a.title_norm = m.title_norm) and (a.title_norm is not null) and (a.title_norm != "") and 
   (a.last_name_norm = m.last_names_norm) and (m.last_names_norm is not null) and (a.last_name_norm != ""))
```

`ds_mag_names_title`

```
select a.id as ds_id, m.id as mag_id
from wos_dim.ds_metadata_clean a
inner join
wos_dim.mag_metadata_clean m
on ((a.title_norm = m.title_norm) and (a.title_norm is not null) and (a.title_norm != "") and 
   (a.last_name_norm = m.last_names_norm) and (m.last_names_norm is not null) and (a.last_name_norm != ""))
```

`wos_mag_names_title`

```
select a.id as wos_id, m.id as mag_id
from wos_dim.wos_metadata_clean a
inner join
wos_dim.mag_metadata_clean m
on ((a.title_norm = m.title_norm) and (a.title_norm is not null) and (a.title_norm != "") and 
   (a.last_names_norm = m.last_names_norm) and (m.last_names_norm is not null) and (a.last_names_norm != ""))
```

`arxiv_ds_names_title`

```
select a.id as arxiv_id, m.id as ds_id
from wos_dim.arxiv_metadata_clean a
inner join
wos_dim.ds_metadata_clean m
on ((a.title_norm = m.title_norm) and (a.title_norm is not null) and (a.title_norm != "") and 
   (a.last_name_norm = m.last_name_norm) and (m.last_name_norm is not null) and (a.last_name_norm != ""))
```

`arxiv_wos_names_title`

```
select a.id as arxiv_id, m.id as wos_id
from wos_dim.arxiv_metadata_clean a
inner join
wos_dim.wos_metadata_clean m
on ((a.title_norm = m.title_norm) and (a.title_norm is not null) and (a.title_norm != "") and 
   (a.last_name_norm = m.last_names_norm) and (m.last_names_norm is not null) and (a.last_name_norm != ""))
```

`wos_ds_names_title`

```
select a.id as wos_id, m.id as ds_id
from wos_dim.wos_metadata_clean a
inner join
wos_dim.ds_metadata_clean m
on ((a.title_norm = m.title_norm) and (a.title_norm is not null) and (a.title_norm != "") and 
   (a.last_names_norm = m.last_name_norm) and (m.last_name_norm is not null) and (a.last_names_norm != ""))
```

#### Abstract-Author Matches

`arxiv_mag_names_abstract`

```
select a.id as arxiv_id, m.id as mag_id
from wos_dim.arxiv_metadata_clean a
inner join
wos_dim.mag_metadata_clean m
on ((a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and (a.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != "") and 
   (a.last_name_norm = m.last_names_norm) and (m.last_names_norm is not null) and (a.last_name_norm != ""))
```

`ds_mag_names_abstract`

```
select a.id as ds_id, m.id as mag_id
from wos_dim.ds_metadata_clean a
inner join
wos_dim.mag_metadata_clean m
on ((a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and (a.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != "") and 
   (a.last_name_norm = m.last_names_norm) and (m.last_names_norm is not null) and (a.last_name_norm != ""))
```

`wos_mag_names_abstract`

```
select a.id as wos_id, m.id as mag_id
from wos_dim.wos_metadata_clean a
inner join
wos_dim.mag_metadata_clean m
on ((a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and (a.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != "") and 
   (a.last_names_norm = m.last_names_norm) and (m.last_names_norm is not null) and (a.last_names_norm != ""))
```

`arxiv_ds_names_abstract`

```
select a.id as arxiv_id, m.id as ds_id
from wos_dim.arxiv_metadata_clean a
inner join
wos_dim.ds_metadata_clean m
on ((a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and (a.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != "") and 
   (a.last_name_norm = m.last_name_norm) and (m.last_name_norm is not null) and (a.last_name_norm != ""))
```

`arxiv_wos_names_abstract`

```
select a.id as arxiv_id, m.id as wos_id
from wos_dim.arxiv_metadata_clean a
inner join
wos_dim.wos_metadata_clean m
on ((a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and (a.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != "") and 
   (a.last_name_norm = m.last_names_norm) and (m.last_names_norm is not null) and (a.last_name_norm != ""))
```

`wos_ds_names_abstract`

```
select a.id as wos_id, m.id as ds_id
from wos_dim.wos_metadata_clean a
inner join
wos_dim.ds_metadata_clean m
on ((a.abstract_trunc_norm_len_filt = m.abstract_trunc_norm_len_filt) and (a.abstract_trunc_norm_len_filt is not null) and (a.abstract_trunc_norm_len_filt != "") and 
   (a.last_names_norm = m.last_name_norm) and (m.last_name_norm is not null) and (a.last_names_norm != ""))
```

#### Now we join stuff; get pairwise mappings

`wos_dim.arxiv_wos_base`

```
select distinct * from (select * from wos_dim.arxiv_wos_title_year
union all
select * from wos_dim.arxiv_wos_abstract_year
union all
select * from wos_dim.arxiv_wos_abstract_title
union all
select * from wos_dim.arxiv_wos_names_title
union all
select * from wos_dim.arxiv_wos_names_abstract)
```

`wos_dim.arxiv_ds_base`

```
select distinct * from (select * from wos_dim.arxiv_ds_title_year
union all
select * from wos_dim.arxiv_ds_abstract_year
union all
select * from wos_dim.arxiv_ds_abstract_title
union all
select * from wos_dim.arxiv_ds_names_title
union all
select * from wos_dim.arxiv_ds_names_abstract)
```

`wos_dim.arxiv_mag_base`

```
select distinct * from (select * from wos_dim.arxiv_mag_title_year
union all
select * from wos_dim.arxiv_mag_abstract_year
union all
select * from wos_dim.arxiv_mag_abstract_title
union all
select * from wos_dim.arxiv_mag_names_title
union all
select * from wos_dim.arxiv_mag_names_abstract)
```

`wos_dim.wos_ds_base`

```
select distinct * from (select * from wos_dim.wos_ds_title_year
union all
select * from wos_dim.wos_ds_abstract_year
union all
select * from wos_dim.wos_ds_abstract_title
union all
select * from wos_dim.wos_ds_names_title
union all
select * from wos_dim.wos_ds_names_abstract)
```

`wos_dim.wos_mag_base`

```
select distinct * from (select * from wos_dim.wos_mag_title_year
union all
select * from wos_dim.wos_mag_abstract_year
union all
select * from wos_dim.wos_mag_abstract_title
union all
select * from wos_dim.wos_mag_names_title
union all
select * from wos_dim.wos_mag_names_abstract)
```

`wos_dim.ds_mag_base`

```
select distinct * from (select * from wos_dim.ds_mag_title_year
union all
select * from wos_dim.ds_mag_abstract_year
union all
select * from wos_dim.ds_mag_abstract_title
union all
select * from wos_dim.ds_mag_names_title
union all
select * from wos_dim.ds_mag_names_abstract)
```

#### Filter Extras

select only the mappings that have ids with exactly one occurrence in the preceding tables. if an article matches more than one other articles, throw it out!


`wos_dim.arxiv_wos`

```
select * from wos_dim.arxiv_wos_base where 
(arxiv_id in (select arxiv_id from (select arxiv_id, count(wos_id) as num_records from wos_dim.arxiv_wos_base group by arxiv_id) where num_records = 1))
and
(wos_id in (select wos_id from (select wos_id, count(arxiv_id) as num_records from wos_dim.arxiv_wos_base group by wos_id) where num_records = 1))
```

`wos_dim.arxiv_ds`

```
select * from wos_dim.arxiv_ds_base where 
(arxiv_id in (select arxiv_id from (select arxiv_id, count(ds_id) as num_records from wos_dim.arxiv_ds_base group by arxiv_id) where num_records = 1))
and
(ds_id in (select ds_id from (select ds_id, count(arxiv_id) as num_records from wos_dim.arxiv_ds_base group by ds_id) where num_records = 1))
```

`wos_dim.arxiv_mag`

```
select * from wos_dim.arxiv_mag_base where 
(arxiv_id in (select arxiv_id from (select arxiv_id, count(mag_id) as num_records from wos_dim.arxiv_mag_base group by arxiv_id) where num_records = 1))
and
(mag_id in (select mag_id from (select mag_id, count(arxiv_id) as num_records from wos_dim.arxiv_mag_base group by mag_id) where num_records = 1))
```

`wos_dim.wos_ds`

```
select * from wos_dim.wos_ds_base where 
(wos_id in (select wos_id from (select wos_id, count(ds_id) as num_records from wos_dim.wos_ds_base group by wos_id) where num_records = 1))
and
(ds_id in (select ds_id from (select ds_id, count(wos_id) as num_records from wos_dim.wos_ds_base group by ds_id) where num_records = 1))
```


`wos_dim.wos_mag`

```
select * from wos_dim.wos_mag_base where 
(wos_id in (select wos_id from (select wos_id, count(mag_id) as num_records from wos_dim.wos_mag_base group by wos_id) where num_records = 1))
and
(mag_id in (select mag_id from (select mag_id, count(wos_id) as num_records from wos_dim.wos_mag_base group by mag_id) where num_records = 1))
```

`ds_dim.ds_mag`

```
select * from wos_dim.ds_mag_base where 
(ds_id in (select ds_id from (select ds_id, count(mag_id) as num_records from wos_dim.ds_mag_base group by ds_id) where num_records = 1))
and
(mag_id in (select mag_id from (select mag_id, count(ds_id) as num_records from wos_dim.ds_mag_base group by mag_id) where num_records = 1))
```

`all_pairs`

```
select distinct * from ((select
  arxiv_id,
  null as wos_id,
  null as ds_id,
  mag_id
from wos_dim.arxiv_mag)
union all
(select
  arxiv_id,
  null as wos_id,
  ds_id,
  null as mag_id
from wos_dim.arxiv_ds)
union all
(select
  arxiv_id,
  wos_id,
  null as ds_id,
  null as mag_id
from wos_dim.arxiv_wos)
union all
(select
  null as arxiv_id,
  wos_id,
  ds_id,
  null as mag_id
from wos_dim.wos_ds)
union all
(select
  null as arxiv_id,
  wos_id,
  null as ds_id,
  mag_id
from wos_dim.wos_mag)
union all
(select
  null as arxiv_id,
  null as wos_id,
  ds_id,
  mag_id
from wos_dim.ds_mag))
```

#### Get triples

`arxiv_wos_ds`

```
(select
  a.arxiv_id,
  b.wos_id,
  b.ds_id,
from (select * from wos_dim.all_pairs where arxiv_id is not null and wos_id is not null) a
inner join
(select * from wos_dim.all_pairs where wos_id is not null and ds_id is not null) b
on a.wos_id = b.wos_id)
```

`arxiv_ds_mag`

```
(select
  a.arxiv_id,
  b.ds_id,
  b.mag_id,
from (select * from wos_dim.all_pairs where arxiv_id is not null and ds_id is not null) a
inner join
(select * from wos_dim.all_pairs where ds_id is not null and mag_id is not null) b
on a.ds_id = b.ds_id)
```

`arxiv_wos_mag`

```
(select
  a.arxiv_id,
  b.wos_id,
  b.mag_id,
from (select * from wos_dim.all_pairs where arxiv_id is not null and wos_id is not null) a
inner join
(select * from wos_dim.all_pairs where wos_id is not null and mag_id is not null) b
on a.wos_id = b.wos_id)
```

`wos_ds_mag`

```
(select
  a.wos_id,
  b.ds_id,
  b.mag_id,
from (select * from wos_dim.all_pairs where wos_id is not null and ds_id is not null) a
inner join
(select * from wos_dim.all_pairs where ds_id is not null and mag_id is not null) b
on a.ds_id = b.ds_id)
```

#### Now start working on full matches

`arxiv_full_matches`

```
select distinct * from ((select
  a.arxiv_id,
  b.wos_id,
  b.ds_id,
  b.mag_id
from wos_dim.all_pairs a
inner join
wos_dim.wos_ds_mag b
on (a.wos_id = b.wos_id) and (a.arxiv_id is not null))
union all
(select
  a.arxiv_id,
  b.wos_id,
  b.ds_id,
  b.mag_id
from wos_dim.all_pairs a
inner join
wos_dim.wos_ds_mag b
on (a.ds_id = b.ds_id) and (a.arxiv_id is not null))
union all
(select
  a.arxiv_id,
  b.wos_id,
  b.ds_id,
  b.mag_id
from wos_dim.all_pairs a
inner join
wos_dim.wos_ds_mag b
on (a.mag_id = b.mag_id) and (a.arxiv_id is not null)))
```

`wos_full_matches`

```
select distinct * from ((select
  b.arxiv_id,
  a.wos_id,
  b.ds_id,
  b.mag_id
from wos_dim.all_pairs a
inner join
wos_dim.arxiv_ds_mag b
on (a.arxiv_id = b.arxiv_id) and (a.wos_id is not null))
union all
(select
  b.arxiv_id,
  a.wos_id,
  b.ds_id,
  b.mag_id
from wos_dim.all_pairs a
inner join
wos_dim.arxiv_ds_mag b
on (a.ds_id = b.ds_id) and (a.wos_id is not null))
union all
(select
  b.arxiv_id,
  a.wos_id,
  b.ds_id,
  b.mag_id
from wos_dim.all_pairs a
inner join
wos_dim.arxiv_ds_mag b
on (a.mag_id = b.mag_id) and (a.wos_id is not null)))
```

`ds_full_matches`

```
select distinct * from ((select
  b.arxiv_id,
  b.wos_id,
  a.ds_id,
  b.mag_id
from wos_dim.all_pairs a
inner join
wos_dim.arxiv_wos_mag b
on (a.arxiv_id = b.arxiv_id) and (a.ds_id is not null))
union all
(select
  b.arxiv_id,
  b.wos_id,
  a.ds_id,
  b.mag_id
from wos_dim.all_pairs a
inner join
wos_dim.arxiv_wos_mag b
on (a.wos_id = b.wos_id) and (a.ds_id is not null))
union all
(select
  b.arxiv_id,
  b.wos_id,
  a.ds_id,
  b.mag_id
from wos_dim.all_pairs a
inner join
wos_dim.arxiv_wos_mag b
on (a.mag_id = b.mag_id) and (a.ds_id is not null)))
```

`mag_full_matches`

```
select distinct * from ((select
  b.arxiv_id,
  b.wos_id,
  b.ds_id,
  a.mag_id
from wos_dim.all_pairs a
inner join
wos_dim.arxiv_wos_ds b
on (a.arxiv_id = b.arxiv_id) and (a.mag_id is not null))
union all
(select
  b.arxiv_id,
  b.wos_id,
  b.ds_id,
  a.mag_id
from wos_dim.all_pairs a
inner join
wos_dim.arxiv_wos_ds b
on (a.wos_id = b.wos_id) and (a.mag_id is not null))
union all
(select
  b.arxiv_id,
  b.wos_id,
  b.ds_id,
  a.mag_id
from wos_dim.all_pairs a
inner join
wos_dim.arxiv_wos_ds b
on (a.ds_id = b.ds_id) and (a.mag_id is not null)))
```

`all_full_matches`

```
select distinct * from 
(select * from wos_dim.arxiv_full_matches
union all
select * from wos_dim.wos_full_matches
union all
select * from wos_dim.ds_full_matches
union all
select * from wos_dim.mag_full_matches)
```

`all_full_matches_filt`

```
select * from wos_dim.all_full_matches where
  (arxiv_id in (select arxiv_id from (select arxiv_id, count(arxiv_id) as num from wos_dim.all_full_matches group by arxiv_id) where num = 1)) and 
  (wos_id in (select wos_id from (select wos_id, count(wos_id) as num from wos_dim.all_full_matches group by wos_id) where num = 1)) and 
  (ds_id in (select ds_id from (select ds_id, count(ds_id) as num from wos_dim.all_full_matches group by ds_id) where num = 1)) and 
  (mag_id in (select mag_id from (select mag_id, count(mag_id) as num from wos_dim.all_full_matches group by mag_id) where num = 1))
```

#### Add three-way pairs with ids that do not occur in full matches to full matches

`all_full_matches_plus_3`

```
select distinct * from
(select * from wos_dim.all_full_matches
union all
--- get the three-way matches that couldn't be fully linked
(select
  arxiv_id,
  wos_id,
  ds_id,
  null as mag_id
from wos_dim.arxiv_wos_ds
where (arxiv_id not in (select arxiv_id from wos_dim.all_full_matches)) and (wos_id not in (select wos_id from wos_dim.all_full_matches)) and (ds_id not in (select ds_id from wos_dim.all_full_matches)))
union all
(select
  arxiv_id,
  wos_id,
  null as ds_id,
  mag_id
from wos_dim.arxiv_wos_mag
where (arxiv_id not in (select arxiv_id from wos_dim.all_full_matches)) and (wos_id not in (select wos_id from wos_dim.all_full_matches)) and (mag_id not in (select mag_id from wos_dim.all_full_matches)))
union all
(select
  arxiv_id,
  null as wos_id,
  ds_id,
  mag_id
from wos_dim.arxiv_ds_mag
where (arxiv_id not in (select arxiv_id from wos_dim.all_full_matches)) and (mag_id not in (select mag_id from wos_dim.all_full_matches)) and (ds_id not in (select ds_id from wos_dim.all_full_matches)))
union all
(select
  null as arxiv_id,
  wos_id,
  ds_id,
  mag_id
from wos_dim.wos_ds_mag
where (wos_id not in (select wos_id from wos_dim.all_full_matches)) and (mag_id not in (select mag_id from wos_dim.all_full_matches)) and (ds_id not in (select ds_id from wos_dim.all_full_matches))))
```

#### Add non-occurring pairs

`all_full_matches_plus_3_plus_2_pairs`

```
(select *
from wos_dim.all_pairs
where ((arxiv_id is not null) and (arxiv_id not in (select arxiv_id from wos_dim.all_full_matches_plus_3 where arxiv_id is not null))) and ((wos_id is not null) and (wos_id not in (select wos_id from wos_dim.all_full_matches_plus_3 where wos_id is not null))))
union all
(select *
from wos_dim.all_pairs
where ((ds_id is not null) and (ds_id not in (select ds_id from wos_dim.all_full_matches_plus_3 where ds_id is not null))) and ((wos_id is not null) and (wos_id not in (select wos_id from wos_dim.all_full_matches_plus_3 where wos_id is not null))))
union all
(select *
from wos_dim.all_pairs
where ((arxiv_id is not null) and (arxiv_id not in (select arxiv_id from wos_dim.all_full_matches_plus_3 where arxiv_id is not null))) and ((mag_id is not null) and (mag_id not in (select mag_id from wos_dim.all_full_matches_plus_3 where mag_id is not null))))
union all
(select *
from wos_dim.all_pairs
where ((ds_id is not null) and (ds_id not in (select ds_id from wos_dim.all_full_matches_plus_3 where ds_id is not null))) and ((mag_id is not null) and (mag_id not in (select mag_id from wos_dim.all_full_matches_plus_3 where mag_id is not null))))
union all
(select *
from wos_dim.all_pairs
where ((ds_id is not null) and (ds_id not in (select ds_id from wos_dim.all_full_matches_plus_3 where ds_id is not null))) and ((arxiv_id is not null) and (arxiv_id not in (select arxiv_id from wos_dim.all_full_matches_plus_3 where arxiv_id is not null))))
union all
(select *
from wos_dim.all_pairs
where ((wos_id is not null) and (wos_id not in (select wos_id from wos_dim.all_full_matches_plus_3 where wos_id is not null))) and ((mag_id is not null) and (mag_id not in (select mag_id from wos_dim.all_full_matches_plus_3 where mag_id is not null)))))
```

`all_full_matches_plus_3_plus_2`

```
select distinct * from
(select * from wos_dim.all_full_matches_plus_3
union all
--- get the two-way matches that couldn't be fully linked
(select * from wos_dim.all_full_matches_plus_3_plus_2_pairs where ((arxiv_id is null) or (arxiv_id in (select arxiv_id from (select arxiv_id, count(arxiv_id) as num from wos_dim.all_full_matches_plus_3_plus_2_pairs group by arxiv_id) where num = 1))) and
((wos_id is null) or (wos_id in (select wos_id from (select wos_id, count(wos_id) as num from wos_dim.all_full_matches_plus_3_plus_2_pairs group by wos_id) where num = 1))) and 
((ds_id is null) or (ds_id in (select ds_id from (select ds_id, count(ds_id) as num from wos_dim.all_full_matches_plus_3_plus_2_pairs group by ds_id) where num = 1))) and
((mag_id is null) or (mag_id in (select mag_id from (select mag_id, count(mag_id) as num from wos_dim.all_full_matches_plus_3_plus_2_pairs group by mag_id) where num = 1)))))
```

#### Add stuff we couldn't match

`all_full_matches_plus_3_plus_2_plus_1`

```
select distinct * from
(select * from wos_dim.all_full_matches_plus_3_plus_2
union all
--- finally, add in the ids we couldn't match to anything
(select
  id as arxiv_id,
  null as wos_id,
  null as ds_id,
  null as mag_id
from wos_dim.arxiv_metadata
where (id not in (select arxiv_id from wos_dim.all_full_matches_plus_3 where arxiv_id is not null)))
union all
(select
  null as arxiv_id,
  id as wos_id,
  null as ds_id,
  null as mag_id
from wos_dim.wos_metadata
where (id not in (select wos_id from wos_dim.all_full_matches_plus_3 where wos_id is not null)))
union all
(select
  null as arxiv_id,
  null as wos_id,
  id as ds_id,
  null as mag_id
from wos_dim.ds_metadata
where (id not in (select ds_id from wos_dim.all_full_matches_plus_3 where ds_id is not null)))
union all
(select
  null as arxiv_id,
  null as wos_id,
  null as ds_id,
  id as mag_id
from wos_dim.mag_metadata
where (id not in (select mag_id from wos_dim.all_full_matches_plus_3 where mag_id is not null))))
```

all_full_matches_plus_3_plus_2_plus_1_filt

 