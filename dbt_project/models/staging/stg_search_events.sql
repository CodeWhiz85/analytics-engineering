select
  cast(event_time as timestamp) as event_time,
  cast(date(event_time) as date) as event_date,
  cast(member_id as varchar) as member_id,
  cast(search_type as varchar) as search_type,
  cast(query as varchar) as query
from raw.search_events
