-- cln_weights.sql
{{ config(materialized='view') }}

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
        date
        , avg(lean_body_mass)   as lean_body_mass
        , avg(bmi)              as bmi
        , avg(weight)           as weight
    from
        src
    where
        1=1
    group by
        1
)

select
    * 
from
    cleaned_data
where
    1=1