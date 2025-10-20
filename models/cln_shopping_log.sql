-- Import file and cast
with src as (
    select
        cast(ID as string)              as id
        , cast(Date as date)            as date
        , cast(Price as numeric)        as price
        , cast(Quantity as numeric)     as quantity
        , cast(Ingredient as string)    as item
    from
        {{source('warehouse', 'shopping_log')}}
    where
        1=1
)

, cleaned_data as (
    select
        *
    from
        src
    where
        1=1
)

select
    * 
from
    cleaned_data
where
    1=1
