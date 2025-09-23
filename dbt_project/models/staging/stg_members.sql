with src as (
  select * from raw.members
)
select
  cast(member_id as varchar) as member_id,
  cast(region as varchar) as region,
  cast(signup_date as date) as signup_date,
  cast(plan as varchar) as plan
from src
