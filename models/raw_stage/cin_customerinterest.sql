with rawtable as (
	select
		t.*
	from {{ source('bronze_lz', 'cin_customerinterest')}} t
	where
		coalesce(t.cin_deleted_at, t.cin_modified_at, t.cin_created_at) > to_date('{{ var('load_date') }}', 'yyyy-MM-dd')
)
select
	t.cin_rowid,
	t.cin_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	t.cin_modified_at,
	cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
	t.cin_deleted_at,
	cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
	-- payload
	t.cin_info,
	-- derrived
	cast(t.cin_rowid as varchar(20)) as cin_bk, -- not used for integration
	case
		when t.cst_customer is null then '$MISSING'
		else coalesce(cst.cst_customer_no, '$UNKNOWN')
	end	as customer_bk
from rawtable t
left outer join {{ source('bronze_lz', 'cst_customer')}} cst
	on cst.cst_rowid = t.cst_customer