select
	t.*,
    coalesce(
		lead(t.load_ts) over(partition by t.hk_servicerequest order by t.load_ts) - interval 1 milliseconds,
		t.srv_deleted_at,
		to_timestamp('2099-12-31 23:59:59.999', 'yyyy-MM-dd HH:mm:ss.SSS')) as loadend_ts,
    coalesce(usrC.usr_login_name, t.usr_created_by) as usr_created_by_login_name,
	coalesce(usrM.usr_login_name, t.usr_modified_by) as usr_modified_by_login_name,
	coalesce(usrD.usr_login_name, t.usr_deleted_by) as usr_deleted_by_login_name
from {{ ref('s_servicerequest') }} t
left outer join {{ ref('r_user') }} usrC
	on usrC.usr_rowid = t.usr_created_by
left outer join {{ ref('r_user') }} usrM
	on usrM.usr_rowid = t.usr_modified_by
left outer join {{ ref('r_user') }} usrD
	on usrD.usr_rowid = t.usr_deleted_by
-- Code Look-ups go here
left outer join {{ ref('vls_codedefinition') }} cdPrio
	on  cdPrio.cog_group = 7
	and cdPrio.cod_value = t.cod_priority
	and cdPrio.cod_language = 10
left outer join {{ ref('vls_codedefinition') }} cdStatus
	on  cdStatus.cog_group = 8
	and cdStatus.cod_value = t.cod_status
	and cdStatus.cod_language = 10