with source as (
    select * from {{ source('raw', 'orders') }}
)
select
    order_id,
    created_at,
    website_session_id,
    user_id,
    primary_product_id,
    items_purchased,
    price_usd,
    cogs_usd,
    price_usd - cogs_usd as margin_usd,
    date_trunc('day', created_at) as order_date,
    date_trunc('month', created_at) as order_month
from source