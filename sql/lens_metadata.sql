SELECT
  scholarly.lens_id AS id,
  MAX(title) as title,
  MAX(abstract) as abstract,
  MAX(LOWER(id.value)) AS clean_doi,
  MAX(year_published) as year,
  ARRAY_AGG(author.last_name) AS last_names,
  ARRAY_AGG(reference.lens_id) AS references
FROM
  lens.scholarly
LEFT JOIN
  UNNEST(external_ids) as id
LEFT JOIN
  UNNEST(authors) as author
LEFT JOIN
  UNNEST(references) as reference
WHERE
  id.type = "doi"
GROUP BY(id)