-- cln_exercise_log_flattened_area.sql
{{ config(materialized='view') }}

-- Import from cln_exercise_log.sql model
with src_flattened as (
    select
        src.date
        , src.exercise_label
        , src.type
        , src.reps
        , src.sets
        , src.volume
        , src.load_multiplier_method
        , src.coeff
        , src.weight_lbs
        , src.body_weight
        , src.volume_load_lbs as volume_load_lbs_total
        , el.weight
        , src.volume_load_lbs * el.weight as volume_load_lbs
        , em.area
        , em.region
        , src.exercise_label_lower
    from
        {{ref('cln_exercise_log')}} as src
    left join {{source('warehouse', 'exercise_loads')}} as el on
        src.exercise_label = el.exercise_name
    left join {{source('warehouse', 'exercise_muscles')}} as em on
        em.name = el.muscle_name
    where 
        1=1
        and src.type != 'Cardio'
        and src.load_multiplier_method != 'Zero-Load'
)

select
    src.*
from
    src_flattened as src
where
    1=1