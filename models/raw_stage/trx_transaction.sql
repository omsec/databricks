with rawtable as (
	select
        trx.*
	from {{ source('bronze_lz', 'trx_transaction')}} trx
    where
        trx.trx_created_at > to_date('{{ var('load_date') }}', 'yyyy-MM-dd')
)
select
	t.trx_rowid,
	t.trx_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	-- payload (denorm & pii) - no references in transactions for legal reasons
	t.trx_transaction_no,
    t.trx_cst_customer_no,
	t.trx_ord_order_no,
	t.trx_created_by,
	t.trx_cst_first_name,
	t.trx_cst_last_name,
	t.trx_emp_first_name,
	t.trx_emp_last_name,
	t.trx_amount,
	t.trx_method,
    -- explicitly address BKs, even when not provided by source (eg. for ghost-record checking)
    cast(t.trx_rowid as varchar(20)) as trx_bk, -- not used for integration (just payload)
    case
		when t.ord_order is null then '$MISSING'
		else coalesce(ord.ord_order_no, '$UNKNOWN')
	end as order_bk,
    case		
		when t.cst_customer is null then '$MISSING'
		else coalesce(cst.cst_customer_no, '$UNKNOWN')
	end	as customer_bk,
	case
		when t.emp_employee is null then '$MISSING'
		else coalesce(emp.emp_employee_no, '$UNKNOWN')
	end as employee_bk
from rawtable t
left outer join {{ ref('ord_order')}} ord
	on ord.ord_rowid = t.ord_order
left outer join {{ ref('cst_customer')}} cst
	on cst.cst_rowid = t.cst_customer
left outer join {{ ref('emp_employee')}} emp
	on emp.emp_rowid = t.emp_employee