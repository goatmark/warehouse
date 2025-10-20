-- Import file and cast
with src as (
    select
        cast(measurement_ID as string)      as id
        , cast(Measurement_Date as date)    as date
        , cast(Weight as numeric)           as weight
        , cast(Lean_Body_Mass as numeric)   as lean_body_mass
        , cast(BMI as numeric)              as bmi
    from
        {{source('warehouse', 'weights')}}
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