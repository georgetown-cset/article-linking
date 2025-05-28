# Article Linking
![Python application](https://github.com/georgetown-cset/article-linking/workflows/Python%20application/badge.svg)

At CSET, we aim to produce a more comprehensive set of scholarly literature by ingesting multiple sources and then
deduplicating articles. This repository contains CSET's current method of cross-dataset article linking. Note that we
use "article" very loosely, although in a way that to our knowledge is fairly consistent across the datasets we draw
from. Books, for example, are included. We currently include articles from arXiv, Papers With Code,
Semantic Scholar, The Lens, and OpenAlex. Some of these sources are largely duplicative (e.g. arXiv is well covered by
other corpora) but are included to aid in linking to additional metadata (e.g. arXiv fulltext).

For more information about the overall merged academic corpus, which is produced using several data pipelines including
article linkage, see the [ETO documentation](https://eto.tech/dataset-docs/mac/).

## Matching articles

To match articles, we need to extract the data that we want to use in matching and put it in a consistent format. The
SQL queries specified in the `sequences/generate_{dataset}_data.tsv` files are run in the order they appear in those
files. For OpenAlex we exclude documents with a `type` of Dataset, Peer Review, or Grant. Finally, for Semantic Scholar,
we exclude any documents that have a non-null
publication type that is one of Dataset, Editorial, LettersAndComments, News, or Review.

For each article in arXiv, Papers With Code, Semantic Scholar, The Lens, and OpenAlex
we [normalized](utils/clean_corpus.py) titles, abstracts, and author last names to remove whitespace, punctuation,
and other artifacts thought to not be useful for linking. For the purpose of matching, we filtered out titles,
abstracts, and DOIs that occurred more than 10 times in the corpus. We then considered each group of articles
within or across datasets that shared at least one of the following (non-null) metadata fields:

*  Normalized title
*  Normalized abstract
*  Citations
*  DOI

as well as a match on one additional field above, or on

*  Publication year
*  Normalized author last names

to correspond to one article in the merged dataset. We also [link](sql/all_match_pairs_with_um.sql) articles based on
vendor-provided cross-dataset links.

## Generating merged articles

Given a set of articles that have been matched together, we [generate](utils/create_merge_ids.py) a single "merged id"
that is linked to all the "original" (vendor) ids of those articles. Some points from our implementation:

* If articles that have been seen in a previous run and were previously assigned to different merged ids are now matched
together, we assign them to a new merged id.
* If a set of articles previously assigned to a given merged id _loses_ articles (either because it is now assigned to
a different merged id, or because it has been deleted from one of the input corpora), we give this set of articles a
new merged id.
* If a set of articles previously assigned to a given merged id _gains_ articles without losing any old articles, we
keep the old merged id for these articles.

This implementation is meant to ensure that downstream pipelines (e.g. model inference, canonical metadata assignment)
always reflect outputs on current metadata for a given merged article regardless of downstream pipeline implementation.

## Automation and output tables

We automate article linkage using Apache Airflow. `linkage_dag.py` contains our current implementation.

* This dag is triggered from the [Semantic Scholar ETL dag](https://github.com/georgetown-cset/semantic-scholar-etl-pipeline/blob/main/s2_dag.py) which runs once a month.
* This dag triggers the [affiliations dag](https://github.com/georgetown-cset/author-affiliations/blob/main/pipeline/affiliations_dag.py).

The DAG generates two tables of analytic significance:

* `staging_literature.all_metadata_with_cld2_lid` - captures metadata for all unmerged articles in a
standard format. It also contains [language ID predictions](utils/run_lid.py) for titles and abstracts based on CLD2.
* `literature.sources` - contains pairs of merged ids and original (vendor) ids linked to those merged ids.

Metadata _selection_ for each merged article happens in a [downstream DAG](https://github.com/georgetown-cset/cset_article_schema).
