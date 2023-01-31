with

source as (

    select * from {{ source('greenery', 'users')}}

)

, final as (
    select
        user_id
        , created_at
        , updated_at
        , address_id
    from source
)

select * from final