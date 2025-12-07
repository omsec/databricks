with rawtable as (
	select
		emp.*
	from {{ source('bronze_lz', 'emp_employee')}} emp
	where
		coalesce(emp.emp_deleted_at, emp.emp_modified_at, emp.emp_created_at) > to_date('{{ var('load_date') }}', 'yyyy-MM-dd')
)
select
	t.emp_rowid,
	t.emp_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	t.emp_modified_at,
	cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
	t.emp_deleted_at,
	cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
	-- payload
	t.emp_employee_no, -- BK
	t.emp_first_name,
	t.emp_last_name,
	t.emp_email,
	t.emp_phone,
	t.emp_birth_date,
	t.emp_hire_date,
	t.emp_manager_id, -- FK
	t.emp_job_title,
	cast(t.usr_account_id as varchar(20)) as usr_account_id, -- FK
	coalesce(empMgr.emp_employee_no, '$OPTIONAL') as manager_bk
from rawtable t
left outer join {{ source('bronze_lz', 'emp_employee')}} empMgr
	on empMgr.emp_rowid = t.emp_manager_id