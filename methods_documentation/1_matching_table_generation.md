## Finding Article Matches 

Having normalized our data, we now need to do within and cross-dataset matches. To do this, we will
create one master table containing all pairs of matches. We will later combine matched articles into
one "canonical" article.

Within a dataset, we have one piece of information that is not (directly) available across datasets: the 
set of references for each article. We will consequently first use this information in our initial
within-dataset merge, and then drop it when doing cross-dataset merges.

For two articles A and B to be considered a match, we require that at least three of the following be true:

- A and B have the same (not null or empty) normalized title
- A and B have the same (not null or empty) normalized abstract
- A and B have the same (not null or empty) pubyear
- A and B have the same (not null or empty) normalized author last names
- A and B have the same (not null or empty) references (if from the same dataset)
- A and B have the same (not null or empty) DOI

To construct these matches, we did the set of queries listed in `sequences/generate_metadata.tsv`

Run: `python3 generate_tables.py <your dataset name> sequences/generate_metadata.tsv`

This results in a match table of pairs of matched documents (including self-matches) which we will use 
in the

\>> [Next Section](methods_documentation/2_merged_table_generation.md)