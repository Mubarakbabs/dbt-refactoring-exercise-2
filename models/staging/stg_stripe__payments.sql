with 
    source as (
        select * from {{ source('stripe', 'payment') }}
    ),

    transformed as (
        select
        *
        from source
        
    )

    select * from transformed