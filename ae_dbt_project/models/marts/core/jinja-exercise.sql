{% set product_categories = ['coffee beans', 'merch', 'brewing supplies'] -%}

select
  date_trunc(sold_at, month) as date_month,
  {% for product_category in product_categories -%}

    sum(case when product_category = '{{product_category}}' then amount end) as {{product_category}}_amount

    {%- if not loop.last -%}
        ,
    {% endif -%}

  {% endfor %}
from {{ source('coffee_shop', 'orders') }} 
group by 1