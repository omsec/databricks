select
	t.hk_order,
	t.hashdiff,
	t.ord_created_at,
	coalesce(usrC.usr_login_name, t.usr_created_by) as usr_created_by_login_name,
	t.ord_modified_at,
	coalesce(usrM.usr_login_name, t.usr_modified_by) as usr_modified_by_login_name,
	t.ord_deleted_at,
	coalesce(usrD.usr_login_name, t.usr_deleted_by) as usr_deleted_by_login_name,
	cod_status,
	coalesce(cdStatusEN.cod_text, t.cod_status) as txt_status_en,
	t.ord_sale_ts,
	t.ord_attr_int,
	t.ord_attr_str,
	t.load_ts,
	t.record_source,
	t.loadend_ts
from {{ ref('v_order') }} t
-- User Look-ups (latest; references are not historized)
left outer join {{ ref('r_user') }} usrC
	on usrC.usr_rowid = t.usr_created_by
left outer join {{ ref('r_user') }} usrM
	on usrC.usr_rowid = t.usr_modified_by
left outer join {{ ref('r_user') }} usrD
	on usrC.usr_rowid = t.usr_deleted_by
-- Code Look-ups (latest)
left outer join {{ ref('v_codedefinition')}} cdStatusEN
	on  cdStatusEN.cog_group = 5
	and cdStatusEN.cod_value = t.cod_status
	and cdStatusEN.cod_language = 10
	and cdStatusEN.loadend_ts = to_timestamp('2099-12-31 23:59:59.999', 'yyyy-MM-dd HH:mm:ss.SSS')
