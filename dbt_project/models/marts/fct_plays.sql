with plays as (
  select * from {{ ref('stg_play_events') }}
), agg as (
  select
    event_date,
    member_id,
    title_id,
    sum(case when action in ('stop','complete') then minutes_watched else 0 end) as minutes_watched,
    max(case when action='complete' then 1 else 0 end) as completed_flag,
    count_if(action='play') as play_starts,
    any_value(device) as any_device
  from plays
  group by 1,2,3
)
select * from agg
