with plays as (
  select * from {{ ref('fct_plays') }}
), sessions as (
  select
    event_date,
    member_id,
    sum(play_starts) as sessions,
    sum(minutes_watched) as minutes_watched,
    sum(completed_flag) as completions
  from plays
  group by 1,2
), searches as (
  select
    event_date,
    member_id,
    count(*) as searches
  from {{ ref('stg_search_events') }}
  group by 1,2
)
select
  coalesce(s.event_date, se.event_date) as event_date,
  coalesce(s.member_id, se.member_id) as member_id,
  coalesce(s.sessions, 0) as sessions,
  coalesce(s.minutes_watched, 0) as minutes_watched,
  coalesce(s.completions, 0) as completions,
  coalesce(se.searches, 0) as searches
from sessions s
full outer join searches se
  on s.event_date = se.event_date and s.member_id = se.member_id
