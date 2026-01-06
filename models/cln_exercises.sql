-- cln_exercises.sql
{{ config(materialized='view') }}

-- Raw exercises cast
with src as (
    select
        cast(src.name as string)                        as exercise_name
        , cast(src.exercise_type as string)             as exercise_type
        , cast(src.coeff as numeric)                    as coeff
        , cast(src.load_multiplier_method as string)    as method
        , cast(src.max_weight__lbs_ as numeric)         as max_weight_lbs  
    from
        {{source('warehouse', 'exercises')}} as src
    where
        1=1 
)

-- Aggregate muscle loads into JSON array per exercise
, src_muscle as (
    select
        cast(exercise_name as string) as exercise_name
        , cast(muscle_name as string) as muscle_name
        , cast(weight as numeric) as weight
    from 
        {{ source('warehouse', 'exercise_loads') }}
    where 
        1=1
        and exercise_name is not null
)

-- Aggregate into JSON array per exercise
, aggregated as (
    select
        exercise_name
        , array_agg(
            struct(
                muscle_name,
                weight
            )
            order by weight desc
        ) as muscles_array
    from
        src_muscle
    where
        1=1
    group by
        exercise_name
) 

-- Consolidate
, final_agg as (
    select
    exercise_name
    , to_json_string(muscles_array) as muscles
from 
    aggregated

)

-- Merge from skeleton defined in source
select
    src.exercise_name
    , src.exercise_type
    , src.coeff
    , src.method
    , src.max_weight_lbs  
    , final_agg.muscles
from
    src
left join final_agg on
    src.exercise_name = final_agg.exercise_name