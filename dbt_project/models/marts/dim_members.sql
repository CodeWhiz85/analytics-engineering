select
  m.member_id,
  m.region,
  m.signup_date,
  m.plan,
  date_diff('day', m.signup_date, current_date) as tenure_days
from {{ ref('stg_members') }} m
