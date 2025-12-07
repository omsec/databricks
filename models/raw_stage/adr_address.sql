with rawtable as (
	select
		t.*
	from {{ source('bronze_lz', 'adr_address')}} t
	where
		coalesce(t.adr_deleted_at, t.adr_modified_at, t.adr_created_at) > to_date('{{ var('load_date') }}', 'yyyy-MM-dd')
)
select
	t.adr_rowid,
	t.adr_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	t.adr_modified_at,
	cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
	t.adr_deleted_at,
	cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
	-- payload
	t.cst_customer,
    t.cod_type,
    t.adr_priority,
    t.adr_address,
    t.adr_primary,
	-- derrived
	cast(t.adr_rowid as varchar(20)) as adr_bk,
	case
		when t.cst_customer is null then '$MISSING'
		else coalesce(cst.cst_customer_no, '$UNKNOWN')
	end	as customer_bk
from rawtable t
left outer join {{ source('bronze_lz', 'cst_customer')}} cst
	on cst.cst_rowid = t.cst_customer