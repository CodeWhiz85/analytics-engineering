select
  t.title_id,
  t.genre,
  t.runtime_minutes,
  t.is_series
from {{ ref('stg_titles') }} t
