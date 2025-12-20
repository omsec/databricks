select
	t.*,
    coalesce(
		lead(t.load_ts) over(partition by t.hk_order order by t.load_ts) - interval 1 milliseconds,
		t.ord_deleted_at,
		to_timestamp('2099-12-31 23:59:59.999', 'yyyy-MM-dd HH:mm:ss.SSS')) as loadend_ts,
    coalesce(usrC.usr_login_name, t.usr_created_by) as usr_created_by_login_name,
	coalesce(usrM.usr_login_name, t.usr_modified_by) as usr_modified_by_login_name,
	coalesce(usrD.usr_login_name, t.usr_deleted_by) as usr_deleted_by_login_name,
	coalesce(cdStatusEN.cod_text, t.cod_status) as txt_status_en
from {{ ref('s_order') }} t
left outer join {{ ref('r_user') }} usrC
	on usrC.usr_rowid = t.usr_created_by
left outer join {{ ref('r_user') }} usrM
	on usrM.usr_rowid = t.usr_modified_by
left outer join {{ ref('r_user') }} usrD
	on usrD.usr_rowid = t.usr_deleted_by
-- Code Look-ups go here
left outer join {{ ref('vls_codedefinition') }} cdStatusEN
	on  cdStatusEN.cog_group = 5
	and cdStatusEN.cod_value = t.cod_status
	and cdStatusEN.cod_language = 10