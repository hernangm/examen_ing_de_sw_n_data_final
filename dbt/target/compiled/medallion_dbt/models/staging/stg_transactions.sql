




with source as (

    select
        -- Identifiers
        cast(transaction_id as varchar)        as transaction_id,
        cast(customer_id as varchar)           as customer_id,

        -- Measures
        cast(amount as double)                 as amount,

        -- Status
        cast(status as varchar)                as status,

        -- Dates / timestamps
        cast(transaction_ts as timestamp)      as transaction_ts,
        cast(transaction_date as date)         as transaction_date

    from read_parquet(
        '/home/usuario/examen_ing_de_sw_n_data_final/data/clean/transactions_20251206_clean.parquet'
    )

)

select *
from source