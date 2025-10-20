select
    cast(key as string)                     as key
    , cast(description as string)           as description
    , cast(date as date)                    as date
    , cast(type as string)                  as type_raw
    , cast(category as string)              as string
    , safe_cast(amount as numeric)          as amount
    , safe_cast(card_last4 as int64)        as card_last4
    , safe_cast(counter as numeric)         as counter
    , safe_cast(intermediate_key as string) as intermediate_key
from
    {{source('warehouse', 'card_transactions')}}
where
    1=1