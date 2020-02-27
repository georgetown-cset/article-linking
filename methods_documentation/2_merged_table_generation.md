## Merging Article Metadata

Our goal in this section is to create two tables. One table will contain a mapping from original
article ids to merged "CSET" ids. The second table will contain a set of merged metadata for each
"CSET" id.

To do the metadata merging, we need to iterate through the set of article matches. For each article, 
we traverse the set of linked articles (or transitively linked articles). We call this a match set. 

We formerly constructed merged metadata for each match set by:

- Selecting an article that has the most metadata (fewest null fields)
- For any null fields, looking through the rest of the articles until we find a not-null field, and
selecting the value in that field

This results in a merged article with as much metadata originating from one source as possible. 

However, a much faster, and almost as good, way to do this is to do the merge in sql by applying some
heuristic for choosing the best metadata version within each individual column. So, at the 
moment we have reverted to a script that simply assigns each match set a CSET id, and writes the matches
out to jsonl. We then BQ load this into the `article_links` table. `article_links` contains two columns, 
`merged_id` and `orig_id`, where `merged_id` is a CSET id, and `orig_id` is an id that can be mapped back 
to the raw data tables.

To generate the `article_links` jsonl, run:

`python3 create_merge_ids.py <directory of exported match pair jsonl> <output match fil>.jsonl all`

Note that run on the full four-way match, this script consumes ~200G of ram while executing. Given that we 
don't expect to run the full match frequently, the current plan is to simply choose
a VM accordingly. :) If this becomes an issue in future, though, we can spend some effort reducing the
memory requirements of the script.

Next, we need to run another sequence file to create the merged metadata. To do this, run:

`python3 generate_tables.py <your dataset name> sequences/generate_merged_metadata.tsv`

This will result in a `article_merged_meta` table containing six columns: `merged_id`, `year`, `title`, 
`abstract`, `author_last_names`, `doi`. It will also generate a `article_links_with_dataset` table 
which adds the original datasets to the `article_links` table. These two tables are what we will
provide as the "user interface" to the linkage tables.

\>> [Next Section](3_reporting.md)