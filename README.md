# Article Linking
![Python application](https://github.com/georgetown-cset/article-linking/workflows/Python%20application/badge.svg)

This repository contains a description and supporting code for CSET's current method of
cross-dataset article linking. Note that we use "article" very loosely, although in a way that to our knowledge
is fairly consistent across corpora. Books, for example, are included.

For each article in arXiv, WOS, Papers With Code, Semantic Scholar, The Lens, and OpenAlex
we normalized titles, abstracts, and author last names. For the purpose of matching, we filtered out
titles, abstracts, and DOIs that occurred more than 10 times in the corpus. We then considered each group of articles
within or across datasets that shared at least one of the following (non-null) metadata fields:

*  Normalized title
*  Normalized abstract
*  Citations
*  DOI

as well as a match on one additional field above, or on

*  Publication year
*  Normalized author last names

to correspond to one article in the merged dataset. We add to this set "near matches" of the concatenation
of the normalized title and abstract within a publication year, which we identify using simhash.

To do this, we run the `linkage_dag.py` on airflow. The article linkage runs weekly, triggered by the `scholarly_lit_trigger` dag.

For an English description of what the dag does, see [the documentation](methods_documentation/overview.md).

### How to use the linkage tables (CSET only)

We have three tables that are most likely to help you use article linkage.

- `gcp_cset_links_v2.article_links` - For each original ID (e.g., from WoS), gives the corresponding CSET ID.
This is a many-to-one mapping. Please update your scripts to use `gcp_cset_links_v2.article_links_with_dataset`,
which has an additional column that contains the dataset of the `orig_id`.

- `gcp_cset_links_v2.all_metadata_with_cld2_lid` - provides CLD2 LID for the titles and abstracts of each
current version of each article's metadata. You can also use this table to get the metadata used in the
match for each version of the raw articles. Note that the `id` column is _not_ unique as some corpora like WOS
have multiple versions of the metadata for different languages.

- `gcp_cset_links_v2.article_merged_metadata` - This maps the CSET `merged_id` to a set of merged metadata.
The merging method takes the maximum value of each metadata field across each matched article, which may not
be suitable for your purposes.
