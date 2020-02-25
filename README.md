# Article Linking

This repository contains a description and supporting code for CSET's current method of 
cross-dataset article linking. There are four parts:

1.) [Metadata generation](methods_documentation/0_metadata_table_generation.md). This section describes
how we normalized a subset of metadata across arXiv, Web of Science, Dimensions, and Microsoft Academic
Graph into a common format, and then normalized that data.

2.) [Matching table generation](methods_documentation/1_matching_table_generation.md). In this section,
we describe how we matched articles across corpora.

3.) [Merged table generation](methods_documentation/2_merged_table_generation.md). Here, we describe how
we took sets of matched articles and combined their metadata.

4.) [Reporting](methods_documentation/3_reporting.md). Finally, we show final counts and overlap
percentages across datasets.
