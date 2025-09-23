with y as (
  select max(event_date) d from marts.fct_member_day_engagement
)
select t.title_id, sum(p.completions) as completions
from marts.fct_plays p
join y on p.event_date = y.d
join marts.dim_titles t using (title_id)
group by 1
order by completions desc
limit 10;
