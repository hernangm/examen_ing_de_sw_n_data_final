{{ config(materialized="view") }}

{% set clean_dir = env_var('CLEAN_DIR') %}
{% set ds_nodash = env_var('DS_NODASH') %}

with source as (

    select
        cast(transaction_id as varchar)   as transaction_id,
        cast(customer_id as varchar)      as customer_id,
        cast(amount as double)            as amount,
        cast(status as varchar)           as status,
        cast(transaction_ts as timestamp) as transaction_ts,
        cast(transaction_date as date)    as transaction_date

    from read_parquet(
        '{{ clean_dir }}/transactions_{{ ds_nodash }}_clean.parquet'
    )

)

select *
from source
