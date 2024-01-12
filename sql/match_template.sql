-- find articles that match on one of the stronger indicators (title, abstract, doi, references) and one other indicator
SELECT
  a.id AS all1_id,
  m.id AS all2_id
FROM {{ staging_dataset }}.all_metadata_norm_filt AS a
INNER JOIN
  {{ staging_dataset }}.all_metadata_norm_filt AS m
  ON (a.{{ params.strong }} = m.{{ params.strong }})
    AND (a.{{ params.strong }} IS NOT NULL) AND (a.{{ params.strong }} != "")
    AND (a.{{ params.other }} = m.{{ params.other }}) AND (a.{{ params.other }} IS NOT NULL) {{ params.additional_checks }}
