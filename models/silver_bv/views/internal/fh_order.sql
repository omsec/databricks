with ts as (
select
	t.hk_order,
	t.hashdiff,
	t.ord_created_at,
	t.usr_created_by_login_name,
	t.ord_modified_at,
	t.usr_modified_by_login_name,
	t.ord_deleted_at,
	t.usr_deleted_by_login_name,
	t.cod_status,
	t.txt_status_en,
	t.ord_sale_ts,
	t.ord_attr_int,
	t.ord_attr_str,
	t.load_ts,
	t.record_source,
	t.loadend_ts
from {{ ref('fe_order') }} t
)
select
from ts
where
	ts.valid_from < ts.valid_to
