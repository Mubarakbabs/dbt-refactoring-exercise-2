with

    -- imports
    orders as (
        select * from {{ ref('stg_jaffle_shop__orders') }}
        ),

    customers as (
        select * from {{ ref('stg_jaffle_shop__customers') }}
        ),

    payments as (
        select * from {{ ref('base_stripe__payments') }}
        ),


    -- logical
    paid_orders as (
        select
            orders.order_id,
            orders.customer_id,
            order_placed_at,
            order_status,
            p.total_amount_paid,
            p.payment_finalized_date
        from orders
        left join payments p on orders.id = p.order_id
    ),

    customer_orders as (
        select
            c.customer_id,
            customer_first_name,
            customer_last_name,
            min(order_date) as first_order_date,
            max(order_date) as most_recent_order_date,
            count(orders.id) as number_of_orders
        from customers c
        left join orders as orders on orders.user_id = c.id
        group by 1,2,3
    ),
    clvs as (
        select 
            p.order_id, 
            sum(t2.total_amount_paid) as clv_bad
        from paid_orders p
        left join
            paid_orders t2
            on p.customer_id = t2.customer_id
            and p.order_id >= t2.order_id
        group by 1
        order by p.order_id)

    select sum (clv_bad) from clvs