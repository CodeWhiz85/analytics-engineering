select
  cast(event_time as timestamp) as event_time,
  cast(date(event_time) as date) as event_date,
  cast(member_id as varchar) as member_id,
  cast(title_id as varchar) as title_id,
  cast(action as varchar) as action,
  cast(minutes_watched as int) as minutes_watched,
  cast(device as varchar) as device
from raw.play_events
