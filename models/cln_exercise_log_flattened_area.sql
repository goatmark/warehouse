-- cln_exercise_log_flattened_area.sql
{{ config(materialized='view') }}

-- Import from cln_exercise_log.sql model
with src as (
    select
        src.date
        , src.exercise_label
        , src.type
        , src.target_areas
        , src.weight_lbs
        , src.reps
        , src.sets
        , src.exercise_label_lower
        , src.volume_load_multiplier
        , src.volume
        , src.volume_load_lbs
        , src.calories
        , src.distance_mi
        , src.duration_min
    from
        {{ref('cln_exercise_log')}} as src
    where
        1=1
)

, flattened_data as (
    select
        src.*
        , trim(area) as target_area
    from src
    cross join unnest(
        split(
        regexp_replace(coalesce(src.target_areas, ''), r'\s*,\s*', ',')
        )
    ) as area
    where 
        1=1
        and trim(area) != ''
)

select
    fd.date
    , fd.target_area
    , fd.exercise_label
    , fd.type
    , fd.weight_lbs
    , fd.reps
    , fd.sets
    , fd.exercise_label_lower
    , fd.volume_load_multiplier
    , fd.volume
    , fd.volume_load_lbs
    , fd.calories
    , fd.distance_mi
    , fd.duration_min
from
    flattened_data fd
where
    1=1
    and fd.type != 'Cardio'
order by
    date
    , exercise_label
    , target_area