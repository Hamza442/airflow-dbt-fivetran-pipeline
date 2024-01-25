{{
config(
  materialized='incremental',
  unique_key='sales_key',
  tags = ["incremental_model"],
  )
}}

with stg_salesorderheader as (
    select
        salesorderid,
        customerid,
        creditcardid,
        shiptoaddressid,
        status as order_status,
        cast(orderdate as date) as orderdate
    from {{ source('FIVETRAN', 'SALESORDERHEADER') }}
),

stg_salesorderdetail as (
    select
        salesorderid,
        salesorderdetailid,
        productid,
        orderqty,
        unitprice,
        unitprice * orderqty as revenue
    from {{ source('FIVETRAN','SALESORDERDETAIL') }}
),
fact_sales as (
select
    {{ dbt_utils.generate_surrogate_key(['stg_salesorderdetail.salesorderid', 'salesorderdetailid']) }} as sales_key,
    p.sk_id as product_key,
    c.sk_id as customer_key,
    a.sk_id as ship_address_key,
    os.sk_id as order_status_key,
    cc.sk_id as creditcard_key,
    stg_salesorderdetail.salesorderid,
    stg_salesorderdetail.salesorderdetailid,
    stg_salesorderdetail.unitprice,
    stg_salesorderdetail.orderqty,
    stg_salesorderheader.orderdate,
    stg_salesorderdetail.revenue
from stg_salesorderdetail
join stg_salesorderheader on stg_salesorderdetail.salesorderid = stg_salesorderheader.salesorderid
left join {{ source('FIVETRAN_SALES','DIM_CUSTOMER') }} c on stg_salesorderheader.customerid=c.customerid
left join {{ source('FIVETRAN_SALES','DIM_ADDRESS') }} a on stg_salesorderheader.shiptoaddressid=a.addressid
left join {{ source('FIVETRAN_SALES','DIM_ORDERSTATUS') }} os on stg_salesorderheader.order_status=os.order_status
left join {{ source('FIVETRAN_SALES','DIM_CREDITCARD') }} cc on stg_salesorderheader.creditcardid=cc.creditcardid
left join {{ source('FIVETRAN_SALES','DIM_PRODUCT') }} p on stg_salesorderdetail.productid=p.productid
)
select * from fact_sales
{% if is_incremental() %}

where orderdate > (select max(orderdate) from {{ this }})

{% endif %}