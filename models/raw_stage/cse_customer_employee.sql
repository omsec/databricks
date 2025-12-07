with rawdata as (
	select *
	from bronze_lz.cse_customer_employee
    where
        coalesce(cse_deleted_at, cse_modified_at, cse_created_at) > to_date('{{ var('load_date') }}', 'yyyy-MM-dd')
)
select
	-- meta
    t.cse_rowid,
	t.cse_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	t.cse_modified_at,
	cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
	t.cse_deleted_at,
	cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
	-- payload
	t.cst_customer,
	t.emp_employee,
	cast(t.cod_role as varchar(20)) as cod_role,
	t.cse_valid_from,
	t.cse_valid_to,
	t.cse_primary,
	-- derrived
	cast(t.cse_rowid as varchar(20)) as cse_bk,
	case
		when t.cst_customer is null then '$MISSING'
		else coalesce(cst.cst_customer_no, '$UNKNOWN')
	end as customer_bk,
	case
		when t.emp_employee is null then '$MISSING'
		else coalesce(emp.emp_employee_no , '$UNKNOWN')
	end as employee_bk
from rawdata t
-- preferrably bronze_ss for the casts
left join {{ ref('cst_customer') }} cst
	on cst.cst_rowid = t.cst_customer
left join {{ ref('emp_employee') }} emp
	on emp.emp_rowid = t.emp_employee