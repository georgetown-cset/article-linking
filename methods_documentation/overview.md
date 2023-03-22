## Normalizing Article Metadata 

We are merging five datasets, all of which are structured differently in our internal database. To
match article metadata, we first need to extract the columns from this data that we want to use
in matching into a consistent set of tables.

To do this, we run the SQL queries specified in the `sequences/generate_{dataset}_data.tsv` sequence files
within our airflow DAG. Mostly this is fairly straightforward, but it's worth noting that for OpenAlex we exclude
documents with a `type` of Dataset, Peer Review, or Grant. Additionally, we take every combination of the WOS
titles, abstracts, and pubyear so that a match on any of these combinations will result in a match on
the shared WOS id.

Having generated the metadata tables, we now need to normalize the metadata. To do this, we use 
the [clean_corpus](../utils/clean_corpus.py) script, which applies several text normalizations to the
data. We "normalize" away even whitespace and punctuation.

Having normalized our data, we now need to do within and cross-dataset matches, creating one master table
containing all pairs of matched articles. To do this, we use the series of queries in 
`sequences/combine_metadata.tsv`.

All of our metadata fields but one can be used to match articles both within and across datasets. The
exception is set of references for each article, which can only be used within dataset. We will
consequently first use this information in our initial within-dataset merge, and then drop it when 
doing cross-dataset merges.

For two articles A and B to be considered a match, we require that at least three of the following be true:

- A and B have the same (not null or empty) normalized title
- A and B have the same (not null or empty) normalized abstract
- A and B have the same (not null or empty) pubyear
- A and B have the same (not null or empty) normalized author last names
- A and B have the same (not null or empty) references (if from the same dataset)
- A and B have the same (not null or empty) DOI

We then add back in any articles that didn't match anything else, and combine the matches into tables that
will be passed to LID and to the simhash and article id assignment code.

For LID, we run the CLD2 library on all titles and abstracts using the beam script in `utils/run_lid.py`, taking
the first language in the output. Note that this can result in the same article being assigned multiple 
languages, since some articles have multiple versions of metadata.

#### Merged article ID assignment

To merge articles, we first need to apply one more matching method, which is based on simhash. On each update
of the data, we update a set of saved simhash indexes (one for each year of the data) that cover all articles 
we have seen on previous runs of the code. We update these indexes with new articles, and then find similar 
articles within the updated indexes. 

Next, we add all the simhash matches as match pairs, and run `utils/create_merge_ids.py`. This script identifies
all groups of articles that have been either directly or transitively matched together. We then assign this set
of articles (the "match set") a "carticle" ID. If a match set has exactly one old carticle id previously assigned 
to any of the articles, it keeps that carticle id even if new articles (with no existing carticle id) are added
to the match set. Otherwise, the match set gets a new carticle id.

Having obtained the carticle ids, we upload them back to BigQuery, and generate the final output tables, 
described in the README.

#### Running LID

In parallel with creating the matches, we run the CLD2 library on all titles and abstracts using the beam 
script in `utils/run_lid.py`. We take the first language in the output as the language of the whole text. 
Note that this can result in the same merged carticle being assigned multiple languages, since some articles 
have multiple versions of metadata.