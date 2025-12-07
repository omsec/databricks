with rawtable as (
	select
		prd.*
	from {{ source('bronze_lz', 'prd_product')}} prd
    where
        coalesce(prd.prd_deleted_at, prd.prd_modified_at, prd.prd_created_at) > to_date('{{ var('load_date') }}', 'yyyy-MM-dd')
)
select
	t.prd_rowid,
	t.prd_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	t.prd_modified_at,
	cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
	t.prd_deleted_at,
	cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
	-- payload
	t.prd_name,
	t.prd_description,
	t.prd_standard_cost,
	t.prd_list_price,
	t.prd_sold_until,
	t.pct_category,
    -- derrived
    cast(t.prd_rowid as varchar(20)) as product_bk,
    coalesce(pct.productcategory_bk, '$OPTIONAL') as productcategory_bk
from rawtable t
left outer join {{ ref('pct_productcategory')}} pct
    on pct.pct_rowid = t.pct_category
    