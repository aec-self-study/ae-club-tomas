with pageviews as (
  select *
  from {{ source('web_tracking', 'pageviews') }}
)

select
  id,
  visitor_id,
  device_type,
  timestamp,
  page,
  customer_id
from pageviews