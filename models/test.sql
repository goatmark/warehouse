select
    ct.description
    , count(*) 
from
    {{ref('cln_card_transactions')}} as ct 
where
    1=1 
    and ct.merchant_key is null
    and ct.type = 'Sale'
    and left(ct.description_lower, 5) != 'venmo'
    and ct.description_lower not like '%automatic payment%'
group by
    1
order by
    2 desc