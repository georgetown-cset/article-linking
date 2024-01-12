-- glue all the ids together (used in validation)
SELECT id FROM {{ staging_dataset }}.arxiv_ids
UNION ALL
SELECT id FROM {{ staging_dataset }}.wos_ids
UNION ALL
SELECT id FROM {{ staging_dataset }}.papers_with_code_ids
UNION ALL
SELECT id FROM {{ staging_dataset }}.openalex_ids
UNION ALL
SELECT id FROM {{ staging_dataset }}.s2_ids
UNION ALL
SELECT id FROM {{ staging_dataset }}.lens_ids
