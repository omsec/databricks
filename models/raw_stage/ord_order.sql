with rawtable as (
	select
		ord.*
	from {{ source('bronze_lz', 'ord_order')}} ord
	where
		coalesce(ord.ord_deleted_at, ord.ord_modified_at, ord.ord_created_at) > to_date('{{ var('load_date') }}', 'yyyy-MM-dd')
)
select
	t.ord_rowid,
	t.ord_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	t.ord_modified_at,
	cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
	t.ord_deleted_at,
	cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
	-- payload
	t.ord_order_no,
	t.ord_sale_ts,
	t.cst_customer,
	t.adr_delivery,
	t.adr_billing,
	t.emp_salesman,
	cast(t.cod_status as varchar(20)) as cod_status,
	t.ord_attr_int,
	t.ord_attr_str,
	-- derrived
	case
		when t.cst_customer is null then '$MISSING'
		else coalesce(cst.cst_customer_no, '$UNKNOWN')
	end	as customer_bk,
	case
		when t.emp_salesman is null then '$MISSING'
		else coalesce(empSm.emp_employee_no, '$UNKNOWN')
	end as salesman_bk,
	case
		when t.adr_delivery is null then '$MISSING'
		else coalesce(adrD.adr_bk, '$UNKNOWN')
	end as adr_delivery_bk,
	case
		when t.adr_billing is null then '$MISSING'
		else coalesce(adrB.adr_bk, '$UNKNOWN')
	end as adr_billing_bk,
	case
		when t.adr_notification is null then '$MISSING'
		else coalesce(adrN.adr_bk, '$UNKNOWN')
	end as adr_notification_bk
from rawtable t
left outer join {{ source('bronze_lz', 'cst_customer') }} cst
	on cst.cst_rowid = t.cst_customer
left outer join {{ source('bronze_lz', 'emp_employee') }} empSm
	on empSm.emp_rowid = t.emp_salesman
left outer join {{ ref('adr_address') }} adrD
	on adrD.adr_rowid = t.adr_delivery
left outer join {{ ref('adr_address') }} adrB
	on adrB.adr_rowid = t.adr_billing
left outer join {{ ref('adr_address') }} adrN
	on adrN.adr_rowid = t.adr_notification