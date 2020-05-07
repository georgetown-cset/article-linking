# Article Linking
![Python application](https://github.com/georgetown-cset/article-linking/workflows/Python%20application/badge.svg)

This repository contains a description and supporting code for CSET's current method of 
cross-dataset article linking. The short version is that  we normalized titles, abstracts, and author last names,
and then considered each group of articles within or across datasets that shared at least three of the following 
(non-null) metadata fields:
 
*  Normalized title
*  Normalized abstract
*  Publication year
*   Normalized author last names
*  Citations (for within dataset matches)
*  DOI
 
to correspond to one article in the merged dataset. We also allow matches returned by simhash for year + a
concatenation of the normalized title and abstract.

To do this, we run the `linkage_dag.py` on airflow, and keep it along with its supporting files up to date
using `./push_to_airflow_bucket.sh`. Don't run that script unless you have pulled and know what you're doing.

-

The documentation comes in five parts: 

1.) [Metadata generation](methods_documentation/0_metadata_table_generation.md). This section describes
how we put a subset of metadata across arXiv, Web of Science, Dimensions, CNKI, and Microsoft Academic
Graph into a common format, and then normalized that data.

2.) [Matching table generation](methods_documentation/1_matching_table_generation.md). In this section,
we describe how we matched articles across corpora using the metadata in (1).

3.) [Merged table generation](methods_documentation/2_merged_table_generation.md). Here, we describe how
we took sets of matched articles and combined their metadata.

4.) [Reporting](methods_documentation/3_reporting.md). Finally, we show final counts and overlap
percentages across datasets.

5.) [How To Use](methods_documentation/4_how_to_use_the_match_tables.md). This is a summary of how to
use the linkage tables.

Note that throughout, we use "article" very loosely, although in a way that to our knowledge is fairly
consistent across corpora. Books, for example, are included. For full details, see the queries in section 1.
