-- with 

-- events as (
--     select session_id, event_type from {{ ref('stg_greenery__events')}}
-- )

-- , session_with_add_to_cart as (
--     select
--         session_id
--         , 
--         CASE
--             WHEN event_type = 'add_to_cart' THEN 1
--             ELSE 0
--         END AS has_add_to_cart
--     from events
--     group by session_id
-- )

-- , final as (
--     select 
--         sum(has_add_to_cart) / count(session_id) 
    
--     from session_with_add_to_cart
-- )

-- select * from final

-- models/marts/add_to_cart_rate.sql

with

events as (

	select * from {{ ref('stg_greenery__events') }}

)

, unique_sessions as (

    select
        count(distinct session_id) as number_of_unique_sessions
        
    from events

)

, unique_add_to_cart_sessions as (

    select
        count(distinct session_id) as number_of_unique_add_to_cart_sessions
        
    from events
    where event_type = 'add_to_cart'

)

, final as (

		select
			  number_of_unique_add_to_cart_sessions
			  , number_of_unique_sessions
			  , number_of_unique_add_to_cart_sessions::float / number_of_unique_sessions::float as add_to_cart_rate
		
		from
				unique_add_to_cart_sessions
				, unique_sessions

)

select * from final