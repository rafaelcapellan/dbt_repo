with STG_CUSTOMER as (

    select * from {{ source('TPCH_SF1', 'CUSTOMER') }}

),

RENAME_STG_CUSTOMER as (

    select
    
        C_CUSTKEY as CUSTOMER_KEY,
        C_NAME as NAME,
        C_ADDRESS as ADDRESS, 
        C_NATIONKEY as NATION_KEY,
        C_PHONE as PHONE_NUMBER,
        C_ACCTBAL as ACCOUNT_BALANCE,
        C_MKTSEGMENT as MARKET_SEGMENT,
        C_COMMENT as COMMENT

    from STG_CUSTOMER

)

select * from RENAME_STG_CUSTOMER