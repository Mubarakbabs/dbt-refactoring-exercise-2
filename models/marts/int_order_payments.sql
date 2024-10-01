with

    orders as (
        select * from {{ ref('stg_jaffle_shop__orders') }}
        ),


    payments as (
        select * from {{ ref('stg_stripe__payments') }}
        ),

    completed_payments as (
        select 
        orderid as order_id,
        max(created) as payment_finalized_date,
        sum(amount) / 100.0 as total_amount_paid
        from all_payments
        where status <> 'fail'
    group by 1
    )

    select
            orders.order_id,
            orders.customer_id,
            order_placed_at,
            order_status,
            
            p.total_amount_paid,
            p.payment_finalized_date
        from orders
        
        left join completed_payments p on orders.id = p.order_id
    