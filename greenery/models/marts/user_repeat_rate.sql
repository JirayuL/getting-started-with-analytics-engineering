-- with users_with_purchase_more_than_one AS (
--   SELECT
--     user_id,
--     CASE
--       WHEN COUNT(order_id) > 1 THEN 'more_than_one_order'
--     END AS purchase_more_than_one
--   FROM
--     {{ ref('stg_greenery__orders')}}
--   GROUP BY
--     user_id
-- )

-- , with user_count as (
--     select count(distinct user_id) as value from {{ ref('stg_greenery__users')}}
-- )

-- SELECT
--   (COUNT(purchase_more_than_one)/COUNT(user_id)) as user_repeate_rate
    -- user_count.value/users_with_purchase_more_than_one.purchase_more_than_one
--     purchase_more_than_one,
--     COUNT(purchase_more_than_one) AS user_count
-- FROM
--   users_with_purchase_more_than_one
-- GROUP BY
--   purchase_more_than_one
with

orders as (

    select * from {{ ref('stg_greenery__orders') }}

)

, user_orders as (

    select
        user_id
        , count(order_id) as order_count
    
    from orders
    group by 1

)

, user_bucket as (

    select
        user_id
        , (order_count >= 2)::int as has_two_orders

    from user_orders

)

, final as (

    select
        sum(has_two_orders)::float / count(distinct user_id)::float as repeat_rate
    
    from user_bucket
    

)

select * from final