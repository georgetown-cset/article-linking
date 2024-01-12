## Normalizing Article Metadata

We are merging five datasets, all of which are structured differently in our internal database. To
match article metadata, we first need to extract the columns from this data that we want to use
in matching into a consistent set of tables.

To do this, we run the SQL queries specified in the `sequences/generate_{dataset}_data.tsv` sequence files
within our airflow DAG. Mostly this is fairly straightforward, but it's worth noting that for OpenAlex we exclude
documents with a `type` of Dataset, Peer Review, or Grant. Additionally, we take every combination of the WOS
titles, abstracts, and pubyear so that a match on any of these combinations will result in a match on
the shared WOS id. Finally, for Semantic Scholar, we exclude any documents that have a non-null publication type
that is one of Dataset, Editorial, LettersAndComments, News, or Review.

Having generated the metadata tables, we now need to normalize the metadata. To do this, we use
the [clean_corpus](../utils/clean_corpus.py) script, which applies several text normalizations to the
data. We "normalize" away even whitespace and punctuation.

Having normalized our data, we now need to do within and cross-dataset matches, creating one master table
containing all pairs of matched articles. To do this, we use the series of queries in
`sequences/combine_metadata.tsv`.

For two articles A and B to be considered a match, we require that they have a non-null match on at least one of:

*  Normalized title
*  Normalized abstract
*  Citations
*  DOI

as well as a match on one additional field above, or on

*  Publication year
*  Normalized author last names

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
