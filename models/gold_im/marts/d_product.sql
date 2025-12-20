-- SCD1: Only apply the latest version (overwrite changes)
-- https://ijaniszewski.medium.com/data-engineering-understanding-slowly-changing-dimensions-scd-with-dbt-98b37f3ddf03

-- Since it's so easy, all dimensions are SCD2 (materialized=incremental) - except for the dim_dates

-- ToDo:
-- SCD1-Demo: read current version of b_product only, then apply incremental or table

{{
    config(
        materialized='incremental',
        unique_key='d_product'
    )
}}

select
	bPrd.hk_product_snapshot as d_product,
	bPrd.hk_product,
	bPrd.product_bk,
	bPrd.valid_from,
	bPrd.valid_to,
	sPrd.prd_name,
	sPrd.prd_standard_cost,
	sPrd.prd_list_price,
	sPrd.prd_sold_until,
	--sPrd.txt_complexity_en
	bPrd.hk_productcategory,
	bPrd.productcategory_bk,
	sPct.pct_name
from{{ ref('b_product') }} bPrd
join{{ ref('vs_product') }} sPrd
	on  sPrd.hk_product = bPrd.hk_product
	and sPrd.load_ts = bPrd.sprd_load_ts
left outer join{{ ref('vs_productcategory') }} sPct
	on  sPct.hk_productcategory = bPrd.hk_productcategory
	and sPct.load_ts = bPrd.spct_load_ts
order by
	bPrd.valid_from,
	cast(bPrd.product_bk as int)