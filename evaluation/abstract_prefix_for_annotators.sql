-- gets 3000 article pairs that share a 100-character abstract prefix but are not matched
CREATE TEMP FUNCTION
  article_desc(title STRING,
    abstract STRING,
    year INT64,
    doi STRING,
    authors STRING)
  RETURNS STRING AS ( CONCAT("title: ", COALESCE(title, ""), "\nyear: ",
      CASE
        WHEN year IS NOT NULL THEN CAST(year AS string)
      ELSE
      ""
    END
      , "\ndoi: ", COALESCE(doi, ""), "\nauthors: ", COALESCE(authors, ""), "\nabstract: ", COALESCE(abstract, "")) );
WITH
  short_abs AS (
  SELECT
    id,
    SUBSTR(abstract, 0, 100) AS short_abs,
    title,
    abstract,
    year,
    CONCAT("https://doi.org/", clean_doi) AS doi,
    STRING_AGG(author, ", ") AS authors
  FROM
    gcp_cset_links_v2.all_metadata_with_cld2_lid
  CROSS JOIN
    UNNEST(last_names) AS author
  WHERE
    LENGTH(abstract) > 100
  GROUP BY
    id,
    title,
    abstract,
    year,
    clean_doi)
SELECT
  set1.id AS orig_id1,
  set2.id AS orig_id2,
  article_desc(set1.title,
    set1.abstract,
    set1.year,
    set1.doi,
    set1.authors) AS metaset1,
  article_desc(set2.title,
    set2.abstract,
    set2.year,
    set2.doi,
    set2.authors) AS metaset2
FROM
  short_abs AS set1
INNER JOIN
  short_abs AS set2
USING
  (short_abs)
INNER JOIN
  gcp_cset_links_v2.article_links AS links1
ON
  (set1.id = links1.orig_id)
INNER JOIN
  gcp_cset_links_v2.article_links AS links2
ON
  (set2.id = links2.orig_id)
WHERE
  set1.id != set2.id
  AND links1.merged_id != links2.merged_id
LIMIT
  3000
