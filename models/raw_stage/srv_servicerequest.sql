with rawtable as (
	select
		srv.*
	from {{ source('bronze_lz', 'srv_servicerequest')}} srv
	where
        coalesce(srv.srv_deleted_at, srv.srv_modified_at, srv.srv_created_at) > to_date('{{ var('load_date') }}', 'yyyy-MM-dd')
)
select
	t.srv_rowid,
	t.srv_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	t.srv_modified_at,
	cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
	t.srv_deleted_at,
	cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
	-- payload
	coalesce(t.srv_request_no, '$MISSING') as srv_request_no,
	t.cst_customer,
	t.srv_subject,
	t.srv_message,
	cast(t.cod_priority as varchar(20)) as cod_priority,
	cast(t.cod_status as varchar(20)) as cod_status,
	t.srv_attr_int,
	t.srv_attr_str,
	-- derrived
	case
		when t.cst_customer is null then '$MISSING'
		else coalesce(cst.cst_customer_no, '$UNKNOWN')
	end	as customer_bk
from rawtable t
left outer join{{ source('bronze_lz', 'cst_customer')}} cst
	on cst.cst_rowid = t.cst_customer