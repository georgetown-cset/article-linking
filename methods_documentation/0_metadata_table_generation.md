## Normalizing Article Metadata 

We are merging four datasets, all of which are structured very differently in our internal database. To
do the metadata merging, we first needed to extract the columns from this data that we wanted to use
in matching into a set of tables with the following columns:

- id
- publication year
- title
- abstract
- author last names
- references
- doi

To do this, we ran the following SQL queries.

**arXiv**


**Web of Science**


**Dimensions**


**Microsoft Academic Graph**


--

Having generated these metadata tables, we then needed to normalize the metadata. To do this, we used
the [clean_corpus](utils/clean_corpus.py) script, which applies a variety of text normalizations to the
data. For the title, abstract, and author last names columns, we generate two versions of the normalized
data, one with and one without the gensim strip_non_alphanumeric function. We will consider a match on
either of these versions of the data to be a valid match.

Having used Beam to write the normalized data back to BigQuery, we are ready to construct the set of
matched data.

\>> [Next Section](methods_documentation/1_matching_table_generation.md)