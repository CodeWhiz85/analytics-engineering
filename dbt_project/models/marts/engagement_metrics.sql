with d as (
  select * from {{ ref('fct_member_day_engagement') }}
), m as (
  select * from {{ ref('dim_members') }}
)
select
  d.event_date,
  count(distinct d.member_id) as dau,
  sum(d.sessions) as total_sessions,
  sum(d.minutes_watched) as total_minutes,
  avg(d.minutes_watched) as avg_minutes_per_active,
  sum(d.completions) as total_completions
from d
join m on m.member_id = d.member_id
group by 1
order by 1 desc
