## How to use the Match Tables

We have two tables that are most likely to help you use article linkage.

- `gcp_cset_links_v2.article_links` - Gives for each original ID (e.g., from WoS) the corresponding CSET ID. This is a many-to-one mapping. Each original id (`orig_id`) is mapped to exactly one CSET id (`merged_id`), but there can be many original ids linked to a CSET id. Of the 330M CSET IDs that exist on 2/27/20, about 130M have a single source publication; 49M represent 2 original IDs; and 31M merge 3 source publications each.

- `gcp_cset_links_v2.article_merged_metadata` - This maps the "CSET" `merged_id` to merged metadata

If you wish to compare _all_ versions of metadata that went into each `merged_id`, you can look at the
development table `documentation_linkage_run.article_links_with_meta`. At the moment, we do not intend
to maintain this table within the `gcp_cset_links_v2` database unless there is demand for it.

