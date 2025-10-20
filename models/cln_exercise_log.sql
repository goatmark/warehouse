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
    from
        {{source('warehouse', 'exercise_log')}}
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
        and date is not null
)

select
    *
from
    cleaned_data