-- cln_ingredient_amounts.sql
{{ config(materialized='view') }}

select
    ia_id
    , recipe_id
    , ingredient_id
    , amount
    , unit
    , cal
    , protein_g
    , fat_g
    , carbs_g
    , fiber_g
    , cost
    , servings
    , safe_divide(cal,       servings) as cal_per_serving
    , safe_divide(protein_g, servings) as protein_g_per_serving
    , safe_divide(fat_g,     servings) as fat_g_per_serving
    , safe_divide(carbs_g,   servings) as carbs_g_per_serving
    , safe_divide(fiber_g,   servings) as fiber_g_per_serving
    , safe_divide(cost,      servings) as cost_per_serving
from {{ source('life', 'ingredient_amounts') }}
where recipe_id is not null
