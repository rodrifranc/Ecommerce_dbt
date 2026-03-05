{{ config(materialized='table') }}

with sessions as (
    select session_date, count(*) as total_sessions
    from {{ ref('stg_sessions') }}
    group by session_date
),
orders as (
    select order_date, count(*) as total_orders,
           sum(price_usd) as revenue, sum(margin_usd) as margin
    from {{ ref('stg_orders') }}
    group by order_date
)
select
    s.session_date as date,
    s.total_sessions,
    coalesce(o.total_orders, 0) as total_orders,
    round(o.total_orders * 100.0 / s.total_sessions, 2) as conversion_rate,
    coalesce(o.revenue, 0) as revenue,
    coalesce(o.margin, 0) as margin
from sessions s
left join orders o on s.session_date = o.order_date