with 
    all_payments as (
        select * from {{ ref('stg_stripe__payments') }}
    )

    select 
        orderid as order_id,
        max(created) as payment_finalized_date,
        sum(amount) / 100.0 as total_amount_paid
        from all_payments
    group by 1