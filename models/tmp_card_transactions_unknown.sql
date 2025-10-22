select
    ct.key
    , ct.date
    , ct.description
    , ct.amount
    , ct.card_last4
    , m.name merchant_name
from
    warehouse.card_transactions ct
left join warehouse.transaction_merchant_maps map on
    ct.key = map.transaction_key
left join warehouse.merchants m on
    m.merchant_key = map.merchant_key
where
    1=1
    and m.merchant_key is null