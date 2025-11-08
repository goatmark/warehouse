-- cln_exercise_log_flattened_area.sql
{{ config(materialized='view') }}

-- Import from cln_exercise_log.sql model
with src_flattened as (
    select
        src.date
        , src.reps
        , src.sets
        , src.type
        , src.calories
        , src.target_areas
        , src.weight_lbs
        , src.distance_mi
        , src.duration_min
        , src.exercise_label
        , src.exercise_label_lower
        , src.coeff
        , src.number_areas
        , src.area_multiplier
        , src.load_multiplier_method
        , src.body_weight
        , src.volume
        , src.volume_load_lbs as volume_load_lbs_total
        , trim(area) as target_area
    from
        {{ref('cln_exercise_log')}} as src
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
    src.date
    , src.exercise_label
    , src.type
    , src.target_areas
    , src.target_area
    , src.reps
    , src.sets
    , src.volume
    , src.weight_lbs
    , src.volume_load_lbs_total
    , src.number_areas
    , src.volume_load_lbs_total * src.area_multiplier as volume_load_lbs
    , src.coeff
    , src.load_multiplier_method
    , src.body_weight
    , src.calories
    , src.distance_mi
    , src.duration_min
    , src.exercise_label_lower

from
    src_flattened as src
where
    1=1
order by
    date
    , exercise_label
    , target_area