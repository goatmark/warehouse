with src_merchants as (
    select
        cast(merchant_key as string)        as merchant_key
        , cast(merchant_name as string)     as merchant_name
        , cast(created_at as timestamp)     as created_at
        , cast(updated_at as timestamp)     as updated_at
    from
        {{source('warehouse', 'merchants')}}
)

select
    *
from
    src_merchants