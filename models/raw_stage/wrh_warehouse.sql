with rawtable as (
	select
		t.*
	from {{ source('bronze_lz', 'wrh_warehouse')}} t
	where
		coalesce(t.wrh_deleted_at, t.wrh_modified_at, t.wrh_created_at) > to_date('{{ var('load_date') }}', 'yyyy-MM-dd')
)
select
	t.wrh_rowid,
	t.wrh_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	t.wrh_modified_at,
	cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
	t.wrh_deleted_at,
	cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
	-- payload
	t.wrh_name,
	t.wrh_capacity,
	-- derrived
	cast(t.wrh_rowid as varchar(20)) as warehouse_bk
from rawtable t