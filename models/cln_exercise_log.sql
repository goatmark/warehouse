-- Import file and cast
with src as (
    select
        cast(id as string)                      as key
        , cast(date as date)                    as date
        , cast(Reps as numeric)                 as reps
        , cast(Sets as numeric)                 as sets
        , cast(Type as string)                  as type
        , cast(Calories as numeric)             as calories
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
        , case
            when
                    type = 'Cardio'
                or  type = 'Calisthenics'
                then 0
            when 
                    exercise_label_lower like '%dumbbell%'
                or  exercise_label_lower like '%dumb bell%'
                or  exercise_label_lower like '%cross body%'
                or  exercise_label_lower = 'arnold press'
                or  exercise_label_lower = 'chest press'
                or  exercise_label_lower = 'hammer curl'
                or  exercise_label_lower = 'shoulder shrug'
                then 2
            else
                1
        end multiplier
    from
        src
    where
        1=1
)

, cleaned_data as (
    select
        *
        , reps * sets as volume
        , reps * sets * weight_lbs as volume_load_lbs_raw
        , reps * sets * weight_lbs * multiplier as volume_load_lbs
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