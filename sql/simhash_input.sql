-- get input for simhash, which is articles with not null titles and abstracts that have not already been matched
SELECT
  id,
  year,
  concat(title_norm, abstract_norm) AS normalized_text
FROM {{ staging_dataset }}.all_metadata_norm_filt
WHERE
  (year IS NOT NULL)
  AND (title_norm IS NOT NULL) AND (title_norm != "")
  AND (abstract_norm IS NOT NULL) AND (abstract_norm != "")
  AND id NOT IN (
    SELECT a.id FROM {{ staging_dataset }}.all_metadata_norm_filt AS a
    LEFT JOIN
      {{ staging_dataset }}.all_metadata_with_cld2_lid_last_run AS b
      ON a.id = b.id
    WHERE (a.title = b.title) AND (a.abstract = b.abstract) AND (a.year = b.year) AND (a.title != "")
      AND (a.title IS NOT NULL) AND (a.abstract != "") AND (a.abstract IS NOT NULL) AND (a.year IS NOT NULL)
  )
