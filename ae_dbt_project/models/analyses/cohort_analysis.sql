{{ config(materialized='table') }}

with customers_all_weeks as (
    select * from {{ ref('customers_all_weeks') }}
),

cohort_weeks as (
    select
        first_week,
        week_number,
        sum(weekly_revenue) as cohort_total_revenue,
        sum(cumulative_revenue) as cohort_cumulative_revenue
    from customers_all_weeks
    group by 1, 2
),

cohort_sizes as (
    select
        first_week,
        count(*) as cohort_size
    from customers_all_weeks
    where week_number = 1
    group by 1
),

normalized_cohort_weeks as (
    select
        *,
        cohort_total_revenue / cohort_size as avg_revenue,
        cohort_cumulative_revenue / cohort_size as avg_ltv
    from cohort_weeks
    left join cohort_sizes using (first_week)
)

select
    *

from normalized_cohort_weeks
where first_week >= '2021-01-03'
order by 1, 2, 3
limit 100