-- Goal: We want to link users' browsing sessions together when 
-- we know that they are in fact the same user.

with stitched_pageviews as (
  select
    *,
    first_value(visitor_id ignore nulls) over (partition by customer_id) as stitched_visitor_id
  from {{ ref('stg_pageviews') }}
)

select
  id,
  stitched_visitor_id as visitor_id,
  device_type,
  timestamp,
  page,
  customer_id
from stitched_pageviews