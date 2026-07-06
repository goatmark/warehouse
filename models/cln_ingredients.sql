-- cln_ingredients.sql
{{ config(materialized='view') }}

select
    ingredient_id
    , name
    , section
    , is_plant
    , plant_name
from {{ source('life', 'ingredients_clean') }}
where name is not null
