-- cln_notion_embeddings.sql
{{ config(materialized='view')}}

with src as (
    select
       src.id                                       as id
       , src.db_id                                  as db_id
       , src.page_id                                as page_id
       , src.title                                  as title
       , array(
           select float64(elem)
           from unnest(json_query_array(src.embedding)) as elem
         )                                          as embedding
       , src.created_at                             as created_at
       , src.snippet                                as snippet
    from
        {{source('warehouse', 'notion_embeddings')}} as src
    where
        1=1
        and src.db_id != 'befcd97e-4ea2-4731-ac28-a4faab5e8959'
)

select
    *
from
    src