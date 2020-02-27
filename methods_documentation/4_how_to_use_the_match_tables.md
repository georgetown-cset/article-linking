## How to use the Match Tables

We have two tables that are most likely to help you use article linkage.

- `gcp_cset_links_v2.article_links` - This maps the "CSET" `merged_id` to the original dataset id (`orig_id`).

- `gcp_cset_links_v2.article_merged_metadata` - This maps the "CSET" `merged_id` to merged metadata

If you wish to compare _all_ versions of metadata that went into each `merged_id`, you can look at the
development table `documentation_linkage_run.article_links_with_meta`. At the moment, we do not intend
to maintain this table within the `gcp_cset_links_v2` database unless there is demand for it.

