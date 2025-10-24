-- cln_card_transactions.sql
{{ config(materialized='view') }}

-- Import file and cast
    -- Note:  field `description_lower` added here to improve downstream query performance
    --    and field `description_clean` added in next CTE for further classification
with src as (
    select
        cast(key as string)                     as transaction_key
        , cast(description as string)           as description
        , cast(date as date)                    as date
        , cast(type as string)                  as type_raw
        , cast(category as string)              as category_raw
        , safe_cast(amount as numeric)          as amount
        , safe_cast(card_last4 as int64)        as card_last4
        , safe_cast(counter as numeric)         as counter
        , safe_cast(intermediate_key as string) as intermediate_key
        , lower(cast(description as string))    as description_lower
    from
        {{source('warehouse', 'card_transactions')}}
    where
        1=1
) 

-- Classify transactions as Payments vs. Sales for analytics
, classified_transactions as (
    select
        -- Original keys
        s.transaction_key
        , s.description
        , s.date
        , s.type_raw
        , s.category_raw
        , s.amount
        , s.card_last4
        , s.counter
        , s.intermediate_key
        , s.description_lower
        -- Introduce description_clean for use in interaccount assignment in next SQ
        , substr(regexp_replace(s.description_lower, '[^a-z]+', ''), 1) as description_clean

        -- Added transformation
        , case
            when s.type_raw is not null then s.type_raw
            when s.card_last4 not in ({{ var('payment_card_last4_list', [3221,4245,5083,6823]) | join(',') }})
                then 'Payment'
            when {{ payments_type_keyword_match('s.description') }} 
                then 'Payment'
            else 'Sale'
        end as type
        , 
    from
        src as s
)

-- Indirect merchant matching subqueries
-- Rationale:
    -- If we have previously classified a Netflix transaction, 
    -- we can re-classify subsequent transactions with the same key
    -- Since keys may change (e.g. American Airlines transactions often include ticket numbers),
    -- we improve this matching further with some normalizing steps
    -- Critical steps:
        -- Fix casing to lowercase
        -- Look only at first 15 letters (no white spaces, numbers commas, special characters)
    -- This means:
        -- If a transaction is directly mapped to a merchant in `transaction_merchant_maps`, OR
        -- A transaction has a norm15 description that already exists in the DB
        -- -> then we can map that merchant to the DB
    -- This prevents us from needing to reclassify the same merchants over and over again! 

, desc_merchant_freq as (
    select
        substr(regexp_replace(lower(s.description), '[^a-z]+', ''), 1, 15) as norm15 -- remove all non-letter characters
        , tm.merchant_key
        , count(*) as hits
    from
        {{ source('warehouse', 'transaction_merchant_maps') }} as tm
    join src as s
        on s.transaction_key = tm.transaction_key
    group by
        1
        , 2
)

, desc_merchant_top as (
    select
        norm15
        , merchant_key
        , hits
        , row_number() over (partition by norm15 order by hits desc, merchant_key) as rn
    from
        desc_merchant_freq
)

-- attach merchant via direct map when present; otherwise infer from top norm15 match
, matched_transactions as (
    select
        ct.transaction_key
        , ct.description
        , ct.date
        , ct.type_raw
        , ct.category_raw
        , ct.amount
        , ct.card_last4
        , ct.counter
        , ct.intermediate_key
        , ct.description_lower
        , ct.description_clean
        , ct.type

        -- New interaccount field
        , {{ is_interaccount('ct.description_clean') }} as is_interaccount
        
        -- Merchant key metadata
        , coalesce(map.merchant_key, dmt.merchant_key) as merchant_key
        , map.merchant_key merchant_key_raw
        , dmt.merchant_key merchant_key_assigned
    from
        classified_transactions as ct
    left join {{ source('warehouse', 'transaction_merchant_maps') }} as map
        on map.transaction_key = ct.transaction_key
    left join desc_merchant_top as dmt
        on substr(regexp_replace(lower(ct.description), '[^a-z]+', ''), 1, 15) = dmt.norm15
        and dmt.rn = 1
)

, enriched_transactions as (
    select
        mt.transaction_key
        , mt.description
        , mt.date
        , mt.type_raw
        , mt.category_raw
        , mt.amount
        , mt.card_last4
        , mt.counter
        , mt.intermediate_key
        , mt.description_lower
        , mt.description_clean
        , mt.is_interaccount
        , case
            when mt.is_interaccount then 'Interaccount'
            when   mt.description_clean like '%chicagoventures%' 
                or mt.description_clean like '%depositid%'
                or mt.description_clean like '%facebookconsumer%'
                or mt.description_clean like '%fedwire%'
                or mt.description_clean like '%fresha%'
                or mt.description_clean like '%interestpayment%'
                or mt.description_clean like '%tegus%'
                or mt.description_clean like '%checkxxxx%'
                or mt.description_clean like '%remoteonline%'
                or mt.description_clean like '%universityofchachdepositppdid%'
                then 'Revenue'
            when mt.description_clean like '%irs%'
             or  mt.description_clean like '%ildeptofrev%'
             or  mt.description_clean like '%ildepofrev%'
                then 'Tax - US'
            when mt.description_clean like '%hmrc%' 
                then 'Tax - UK'
            else 'Expense'
        end transaction_type
        , mt.type
        , mt.merchant_key
        , mt.merchant_key_raw
        , mt.merchant_key_assigned

        -- New merchant dimensions
        , m.merchant_name as merchant
        , m.category_id
        , m.category_name
        , m.subcategory_id
        , m.subcategory_name
    from
        matched_transactions as mt
    left join {{ ref('cln_merchants') }} as m
        on m.merchant_key = mt.merchant_key
)

select
    t.transaction_key

    -- dimensions
    , t.date
    , t.merchant
    , t.transaction_type
    , t.category_name as category
    , t.subcategory_name as subcategory
    , t.description
    , t.is_interaccount
    , t.card_last4
    , t.amount

    -- intermediate columns
    , t.type type_old
    , t.merchant_key_raw
    , t.merchant_key_assigned
    , t.category_raw
    , t.type_raw
    , t.description_lower
    , t.description_clean
    , t.intermediate_key
    , t.counter

    -- foreign keys
    , t.merchant_key
    , t.category_id
    , t.subcategory_id
from
    enriched_transactions as t
where
    1=1
order by
    t.date desc