-- Goal: We want to create a table of pre-calculated
-- cumulative revenue values for all of our customers
-- so that we can easily build these cohorted LTV curves.


-- Exercise: Create a table that has one row per customer per week since that user was acquired. 

{{ config(materialized='table') }}

with purchases as (
    select distinct
        customers.id as customer_id
        , concat(cast((extract(week from orders.created_at) + 1) as string)
                , '-'
                , cast(extract(year from orders.created_at) as string)
        ) as week_year
        , sum(product_prices.price) as revenue
    from {{ source('coffee_shop', 'customers') }} as customers
    join {{ source('coffee_shop', 'orders') }} orders
        on customers.id = orders.customer_id
    join {{ source('coffee_shop', 'order_items') }} as order_items
        on orders.id = order_items.order_id
    join {{ source('coffee_shop', 'products') }} as products
        on order_items.product_id = products.id
    join {{ source('coffee_shop', 'product_prices') }} as product_prices
        on products.id = product_prices.product_id
    group by customers.id, orders.created_at
)

select customer_id, week_year, revenue
    , sum(revenue) over (partition by customer_id order by week_year rows between unbounded preceding and current row) as cumulative_revenue
from purchases
order by customer_id, week_year