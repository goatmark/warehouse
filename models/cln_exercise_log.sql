-- cln_exercise_log.sql
{{ config(materialized='view') }}

-- Import file and cast
with src as (
    select
        cast(id as string)                      as key
        , cast(date as date)                    as date
        , cast(Reps as int64)                   as reps
        , cast(Sets as int64)                   as sets
        , cast(Type as string)                  as type
        , cast(Calories as int64)               as calories
        , cast(Target_Areas as string)          as target_areas
        , cast(Weight__lbs_ as numeric)         as weight_lbs
        , cast(Distance__mi_ as numeric)        as distance_mi
        , cast(Duration__min_ as numeric)       as duration_min
        , cast(Exercise_Label as string)        as exercise_label
        , lower(cast(Exercise_Label as string)) as exercise_label_lower
    from
        {{source('warehouse', 'exercise_log')}}
    where
        1=1
)

, multiplier_data as (
    select
        src.*
        -- Rationale:
            -- We only calculate volume loads (reps x sets x weights) on Weights
            -- For dumbbell-based exercises, weight of dumbbell is e.g. 30 lb x 2 -> 60 lbs total (2x)
            -- For all other exercises, weight of e.g. barbell is same as weight of total load (1x) 
        , case
            when
                    src.type = 'Cardio'
                or  src.type = 'Calisthenics'
                then 0
            when 
                    src.exercise_label_lower like '%dumbbell%'
                or  src.exercise_label_lower like '%dumb bell%'
                or  src.exercise_label_lower like '%cross body%'
                or  src.exercise_label_lower = 'arnold press'
                or  src.exercise_label_lower = 'chest press'
                or  src.exercise_label_lower = 'hammer curl'
                or  src.exercise_label_lower = 'shoulder shrug'
                then 2
            else
                1
        end as volume_load_multiplier
    from
        src
    where
        1=1
)

, cleaned_data as (
    select
        *
        , reps * sets                                           as volume
        , reps * sets * weight_lbs * volume_load_multiplier     as volume_load_lbs
    from
        multiplier_data
    where
        1=1
        and date is not null
)

select
    * 
from
    cleaned_data
where
    1=1