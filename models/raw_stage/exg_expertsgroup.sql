with rawtable as (
	select
		exg.*
	from {{ source('bronze_lz', 'exg_expertsgroup')}} exg
	where
		coalesce(exg.exg_deleted_at, exg.exg_modified_at, exg.exg_created_at) > to_date('{{ var('load_date') }}', 'yyyy-MM-dd')
)
select
	t.exg_rowid,
	t.exg_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	t.exg_modified_at,
	cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
	t.exg_deleted_at,
	cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
	-- payload
	t.exg_name,
	t.exg_description,
	t.emp_manager,
	t.exg_active,
	--cast(t.cod_status as varchar(20)) as cod_status,
	-- derrived
	case
		when t.emp_manager is null then '$OPTIONAL'
		else coalesce(empMgr.emp_employee_no, '$UNKNOWN')
	end as manager_bk,
    -- explicitly handle BKs, even when not provided by source (eg. for ghost-record checking)
    cast(t.exg_rowid as varchar(20)) as exg_group_bk
from rawtable t
left outer join {{ source('bronze_lz', 'emp_employee')}} empMgr
	on empMgr.emp_rowid = t.emp_manager