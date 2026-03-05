{{ config(materialized='table') }}

with orders as (select * from {{ ref('stg_orders') }}),
     sessions as (select * from {{ ref('stg_sessions') }}),
     products as (select * from {{ source('raw', 'products') }})

select
    o.order_id, o.created_at, o.order_date, o.order_month,
    o.user_id, o.items_purchased, o.price_usd, o.cogs_usd, o.margin_usd,
    s.utm_source, s.utm_campaign, s.device_type, s.is_repeat_session,
    p.product_name as primary_product_name,
    case
        when o.price_usd > 100 then 'High Value'
        when o.price_usd >= 50 then 'Medium Value'
        else 'Low Value'
    end as order_tier
from orders o
left join sessions s on o.website_session_id = s.website_session_id
left join products p on o.primary_product_id = p.product_id