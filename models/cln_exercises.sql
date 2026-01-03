-- cln_exercises.sql
{{ config(materialized='view') }}

-- Aggregate muscle loads into JSON array per exercise
with src as (
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
        src
    where
        1=1
    group by
        exercise_name
) 

select
    exercise_name
    , to_json_string(muscles_array) as muscles
from 
    aggregated