with

    -- imports
    orders as (
        select * from {{ ref('int_order_payments') }}
        ),

    customers as (
        select * from {{ ref('stg_jaffle_shop__customers') }}
        ),

--final 
    final_cte as (
    select
        p.order_id,

        p.customer_id,

        p.order_placed_at,

        p.order_status,

        p.total_amount_paid,

        p.payment_finalized_date,

        customer_first_name,

        customer_last_name,

        row_number() over (order by p.order_placed_at, p.order_id) as transaction_seq,

        row_number() over (
            partition by customer_id order by p.order_placed_at, p.order_id
            ) as customer_sales_seq,

        
        case 
        when (
            rank() over (
                partition by p.customer_id
                order by p.order_placed_at, p.order_id
                ) = 1
            ) then 'new'
            else 'return' end as nvsr,

        sum(total_amount_paid) over (
            partition by customer_id
            order by p.order_placed_at, p.order_id
        ) as customer_lifetime_value,

        first_value(order_placed_at) over (
            partition by customer_id 
            order by order_placed_at
            ) as fdos,

    from orders p
    left join customers as c using (customer_id)
    order by order_id
    )

    select * from final_cte