Picking up from 0_data_preparation, this document describes our experimental partitions.

`sample_5K` was a quick sample of 5K DOI-matched articles. With commit 85587ce29149ee4c17b2b96652513f84a25a8653, got:

system_name,precision,recall,f1
wos_dim_article_linking.50K_plus_50K_eval_titlefilter4,0.9746122731060837,0.95896,0.9667227839551599

--

`sample_100K` was a larger sample of 100K DOI-matched articles. With commit 85587ce29149ee4c17b2b96652513f84a25a8653, got

system_name,precision,recall,f1
wos_dim_article_linking.100K_titlefilter3,0.996761937223687,0.99428,0.9955194216799915

--

These results are too good, however. We need to concentrate our eval on the non-trivial cases. We can make
this considerably more difficult by:

a.) Filtering out the trivial exact matches:

```
select * from wos_dim_article_linking.clean_filtered_doi_match_with_authors where
(not (ds_title = wos_title)) or (not (ds_abstract = wos_abstract))
```

writing to: `wos_dim_article_linking.nontrivial_subset_clean_filtered_dois`

b.) Increasing the number of "unmatchable" DS IDs to simulate what we have in our actual data.

We know that:

select count(distinct(id)) from gcp_cset_clarivate.cset_wos_corpus
returns
49028044

select count(distinct(id)) from gcp_cset_digital_science.dimensions_publications_with_abstracts_latest
returns
107308436

So we'll aim for about 50-50 matchable to unmatchable IDs in our eval set.

Get the 50K with a match:

```
select * from wos_dim_article_linking.nontrivial_subset_clean_filtered_dois limit 50000
```

writing to: `wos_dim_article_linking.50K_nontrivial_sample`

Then get 50K without a match:

```
select * from wos_dim_article_linking.nontrivial_subset_clean_filtered_dois where wos_id not in
(select wos_id from wos_dim_article_linking.50K_nontrivial_sample)
limit 50000
```

writing to: `wos_dim_article_linking.50K_nontrivial_sample_confusions`

Then add the two sets together:

```
select * from wos_dim_article_linking.50K_nontrivial_sample
union all
select * from wos_dim_article_linking.50K_nontrivial_sample_confusions
```

writing to: `wos_dim_article_linking.50K_plus_50K_eval_set`

Our script pickles WOS and then tries to match it against DS. So we'll pickle only the `50K_nontrivial_sample`
ids, match against the full `50K_plus_50K_eval_set`, and calculate performance wrt the `50K_nontrivial_sample`
matches.

With commit 35e927cf5abf57d4c03079653cae364b5d7fce66 , even this set gets

system_name,precision,recall,f1
wos_dim_article_linking.50K_plus_50K_eval_titlefilter,0.9746122731060837,0.95896,0.9667227839551599



