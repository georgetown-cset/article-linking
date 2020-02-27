## Results

Our current method is extremely simple. Nevertheless, we were able to match the following
percentages of the datasets to each other (self-matches included):

arXiv-WOS | 49.33%

arXiv-DS | 99.68%

arXiv-MAG | 93.32%

WOS-DS | 66.40%

WOS-MAG | 70.52%

DS-MAG | 75.96%

Resulting in 213,965,351 total articles.

[This document](https://docs.google.com/document/d/1kAPWzivVVGoDNR-zzrb8QANuSqv2RcEXD72NjWQHdfg/edit) delves
into the current results in more detail.

In future work, we will resume exploration of more computationally intensive string similarity matching
that would allow for "fuzzier" matches. Our current strategy, if nothing else, has the virtue of being
relatively unambiguous and cheap to run.

In the more immediate future, however, we plan to focus on applying Language ID to this merged corpus
to improve its usability. Some of the constituent corpora (DS and WOS) have some form of language ID, but
a cursory exploration reveals errors. 

We also need to figure out how to deal with articles that may have e.g. abstracts in multiple languages.
At the moment, the longest abstract is chosen with no regard for language ID. Realistically, for some
applications we will want the English version (if it exists), and for others we will want the 
foreign-language version.

In the next section, we recap how to use the match tables.

\>> [Next Section](4_how_to_use_the_match_tables.md)