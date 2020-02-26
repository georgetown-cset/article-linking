## Merging Article Metadata

Our goal in this section is to create two tables. One table will contain a mapping from original
article ids to merged "CSET" ids. The second table will contain a set of merged metadata for each
"CSET" id.

To do the metadata merging, we need to iterate through the set of article matches. For each article, 
we traverse the match set, finding all linked articles. We call this a match set. 

We formerly constructed merged metadata for each match set by:

- Selecting an article that has the most metadata (fewest null fields)
- For any null fields, looking through the rest of the articles until we find a not-null field, and
selecting the value in that field

This results in a merged article with as much metadata as possible. 

However, a much faster, and almost as good, way to do this is to simply do the merge in sql. We'll join


We now, to reduce later confusion,
assign this article a CSET id, and output two tables:

- article_links: two columns, `merged_id` and `orig_id`. `merged_id` is a CSET id, and `orig_id` is an id
that can be mapped back to the raw data tables.
- merged_meta: seven columns, `merged_id`, `year`, `title`, `abstract`, `author_last_names`, `doi`

The script we use to do this is `create_canonical_metadata.py`. This script loads the entire metadata
and match tables into memory, and consequently requires a lot of RAM. We have successfully run it on a
(todo:name) VM with 96 CPUs and 1.32 T of RAM.

\>> [Next Section](methods_documentation/3_reporting.md)