
  
  create view "medallion"."main"."stg_transactions__dbt_tmp" as (
    




with source as (

    select
        cast(transaction_id as varchar)   as transaction_id,
        cast(customer_id as varchar)      as customer_id,
        cast(amount as double)            as amount,
        cast(status as varchar)           as status,
        cast(transaction_ts as timestamp) as transaction_ts,
        cast(transaction_date as date)    as transaction_date

    from read_parquet(
        '/home/usuario/examen_ing_de_sw_n_data_final/data/clean/transactions_20251206_clean.parquet'
    )

)

select *
from source
  );
