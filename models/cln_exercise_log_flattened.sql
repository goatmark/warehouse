-- Import from cln_exercise_log.sql model
with src as (
    select
        *
    from
        {{ref('cln_exercise_log')}}
    where
        1=1
)

, flattened_data as (
    select
        src.*
        , lower(trim(src.target_areas)) as target_area_norm
    from src,
    unnest(
        split(
        regexp_replace(coalesce(src.target_areas, ''), r'\s*,\s*', ',')
        )
    ) as area
    where 
        1=1
        and trim(target_areas) != ''
)

select
    *
from
    flattened_data
