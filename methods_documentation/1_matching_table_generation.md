## Finding Article Matches 

Having normalized our data, we now need to do within and cross-dataset matches. To do this, we will
create one master table containing all pairs of matches. We will later combine matched articles into
one "canonical" article.

Within a dataset, we have one piece of information that is not (directly) available across datasets: the 
set of references for each article. We will consequently first use this information in our initial
within-dataset merge, and then drop it when doing cross-dataset merges.

For two articles A and B to be considered a match, we require that at least three of the following be true:

- A and B have the same normalized title (by either method)
- A and B have the same normalized abstract (by either method)
- A and B have the same pubyear
- A and B have the same normalized author last names (by either method)
- A and B have the same references (if from the same dataset)
- A and B have the same DOI

To construct these matches, we did the following set of queries:

Resulting in a match table which we will use in the

\>> [Next Section](methods_documentation/2_merged_table_generation.md)