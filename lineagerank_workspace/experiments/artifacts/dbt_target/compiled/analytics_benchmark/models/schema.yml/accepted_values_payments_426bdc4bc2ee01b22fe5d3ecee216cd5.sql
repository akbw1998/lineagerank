
    
    

with all_values as (

    select
        payment_method as value_field,
        count(*) as n_records

    from "analytics_benchmark"."main"."payments"
    group by payment_method

)

select *
from all_values
where value_field not in (
    'credit_card','debit_card','paypal','gift_card'
)


