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
        , cast(Coeff as numeric)                as coeff
        , cast(Number_Areas as numeric)         as number_areas
        , cast(Area_Multiplier as numeric)      as area_multiplier
        , cast(Load_Multiplier_Method as string)as load_multiplier_method
    from
        {{source('warehouse', 'exercise_log')}}
    where
        1=1
)

-- The following three CTEs are for a) creating a date vector and b) forward-filling 
-- weight values for missing data
-- This is used to calculate the load of calisthenic / counterweight exercises
-- If this ends up having utility outside of the exercise log, this should be moved into 
-- an int_weights model  

-- 1. Generate date vector
, date_vector as (
    select
        *
    from
        unnest(generate_date_array(
        date_trunc(date '2021-10-25', day), -- Earliest measurement 
        date_trunc(current_date(), day),    -- Current date
        interval 1 day
      )) as date

)

-- 2. Consolidate same-day weight measurements
, weights_data as (
    select
        w.measurement_date as date
        , avg(w.weight) as weight
    from
        {{source('warehouse', 'weights')}} as w
    where
        1=1
    group by
        1
)

-- 3. Forward fill weight by date
, filled_weight as (
    select
        dv.date
        , last_value(w.weight ignore nulls) over (order by dv.date
          rows between unbounded preceding and current row) as weight
    from
        date_vector as dv
    left join weights_data w on
        w.date = dv.date
)

, cleaned_data as (
    select
        -- Retained Metrics
        src.key
        , src.date
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
    
        -- Weight Data
        , w.weight                                              as body_weight

        -- Volume Metrics
        , src.reps * src.sets                                   as volume
        , case
            when src.load_multiplier_method = 'Zero-Load'
                then 0
            when src.load_multiplier_method = 'Weight Coefficient'
                then src.reps * src.sets * src.weight_lbs * src.coeff
            when src.load_multiplier_method = 'Counterweight'
                then src.reps * src.sets * (w.weight - coalesce(src.weight_lbs, 0)) * src.coeff
        end volume_load_lbs
    from
        src
    left join filled_weight w on
        w.date = src.date
    where
        1=1
)

select
    * 
from
    cleaned_data
where
    1=1