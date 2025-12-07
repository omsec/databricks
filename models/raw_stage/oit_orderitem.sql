with rawtable as (
	select
		oit.*
	from {{ source('bronze_lz', 'oit_orderitem')}} oit
	where
		coalesce(oit.oit_deleted_at, oit.oit_modified_at, oit.oit_created_at) > to_date('{{ var('load_date') }}', 'yyyy-MM-dd')
)
select
	t.oit_rowid,
	t.oit_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	t.oit_modified_at,
	cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
	t.oit_deleted_at,
	cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
	-- payload
	t.ord_order,
	t.prd_product,
	t.wrh_warehouse,
	t.oit_quantity,
	t.oit_unit_price,
	t.oit_attr_int,
	t.oit_attr_str,
	-- derrived
	cast(t.oit_rowid as varchar(20)) as orderitem_bk,
	case
		when t.ord_order is null then '$MISSING'
		else coalesce(ord.ord_order_no, '$UNKNOWN')
	end as order_bk,
	case
		when t.prd_product is null then '$MISSING'
		else coalesce(prd.product_bk, '$UNKNOWN')
	end as product_bk,
	case
		when t.wrh_warehouse is null then '$MISSING'
		else coalesce(wrh.warehouse_bk, '$UNKNOWN')
	end as warehouse_bk
from rawtable t
left outer join {{ ref('ord_order')}} ord
	on ord.ord_rowid = t.ord_order
left outer join {{ ref('prd_product')}} prd
	on prd.prd_rowid = t.prd_product
left outer join {{ ref('wrh_warehouse' )}} wrh
	on wrh.wrh_rowid = t.wrh_warehouse