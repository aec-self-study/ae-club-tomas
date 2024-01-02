-- Define a session as a series of page views that are not separated by a gap of more than 30 minutes.

with lag_time_dif as (
    select 
        id,
        visitor_id,
        device_type,
        timestamp,
        page,
        customer_id,
        timestamp_diff(timestamp, LAG(timestamp) over (partition by customer_id order by timestamp), minute) as lag_timestamp_dif
    from {{ ref('stg_pageviews') }}
)

, create_session_id as (
    select 
        id,
        visitor_id,
        device_type,
        timestamp,
        page,
        customer_id,
        sum(case when lag_timestamp_dif > 30 then 1 else 0 end) over (partition by customer_id order by timestamp) as session_id
    from lag_time_dif
)

select 
    id,
    visitor_id,
    device_type,
    timestamp,
    page,
    customer_id,
    session_id,
    min(timestamp) over (partition by customer_id, session_id) as session_start_at,
    max(timestamp) over (partition by customer_id, session_id) as session_end_at
from create_session_id        
