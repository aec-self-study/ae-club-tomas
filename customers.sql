select 
  customers.id, 
  customers.name,
  customers.email,
  min(orders.created_at) as first_order_at,
  count(orders.id) as number_of_orders
from 
  `analytics-engineers-club.coffee_shop.customers` customers
  join `analytics-engineers-club.coffee_shop.orders` orders
    on customers.id = orders.customer_id
group by
  1,2,3
order by
  first_order_at
limit 5

