select 
  customers.id, 
  customers.name,
  customers.email,
  min(o.created_at) as first_order_at,
  count(o.id) as number_of_orders
from 
  `analytics-engineers-club.coffee_shop.customers` customers
  join `analytics-engineers-club.coffee_shop.orders` o
    on customers.id = o.customer_id
group by
  customers.id, 
  customers.name,
  customers.email,
order by
  first_order_at
limit 5

