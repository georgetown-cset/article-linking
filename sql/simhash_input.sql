select
  id,
  year,
  concat(title_norm, abstract_norm) as normalized_text
from {{params.dataset}}.all_metadata_norm
where
    (year is not null) and
    (title_norm is not null) and (title_norm != "") and
    (abstract_norm is not null) and (abstract_norm != "")