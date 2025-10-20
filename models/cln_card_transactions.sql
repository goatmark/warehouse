select
    ct.key
    , ct.description
    , ct.date
    , ct.type
    , ct.amount
    , ct.category
    , ct.card_last4
    , ct.counter
    , ct.number_instances
    , ct.intermediate_key 
from
    warehouse.card_transactions as ct
where
    1=1
order by
    ct.date desc