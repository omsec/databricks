select
  pCst.hk_customer_snapshot,
	pCst.hk_customer,
	pCst.cst_customer_no,
	pCst.valid_from,
	pCst.valid_to,
	sCstM.cst_created_at,
	sCstM.usr_created_by,
	coalesce(usrC.usr_login_name, sCstM.usr_created_by) as usr_created_by_login_name,
	sCstM.cst_modified_at,
	sCstM.usr_modified_by,
	coalesce(usrM.usr_login_name, sCstM.usr_modified_by) as usr_modified_by_login_name,
	sCstM.cst_deleted_at,
	sCstM.usr_deleted_by,
	coalesce(usrD.usr_login_name, sCstM.usr_deleted_by) as usr_deleted_by_login_name,
	sCst.cod_gender,
	coalesce(cdGender.cod_text, sCst.cod_gender) as txt_gender_en,
	sCst.cst_first_name,
	sCst.cst_last_name,
	sCst.cst_birth_date,
	sCst.cod_language,
	coalesce(cdLanguage.cod_text, sCst.cod_language) as txt_language_en,
	sCst.cst_culture,
	sCst.cst_credit_limit,
	sCstExt.cst_remark,
	sCstExt.cst_attr1,
	sCstExt.cst_attr2,
	sCstExt.cst_attr3,
	sCstExt.cst_attr_int,
	sCstExt.cst_attr_str
from {{ ref('p_customer') }} pCst
-- using outer joins, since we deal with different history-lines
left outer join {{ ref('v_customer_meta') }} sCstM
	on  pCst.hk_customer = sCstM.hk_customer
	and pCst.sCstM_load_ts = sCstM.load_ts
left outer join {{ ref('s_customer') }} sCst
	on  sCst.hk_customer = pCst.hk_customer
	and sCst.load_ts = pCst.sCst_load_ts
left outer join {{ ref('v_customer_extended') }} sCstExt
	on  sCstExt.hk_customer = pCst.hk_customer
	and sCstExt.load_ts = pCst.sCstExt_load_ts
-- User Look-ups (latest; references are not historized)
left outer join {{ ref('r_user') }} usrC
  on usrC.usr_rowid = sCstM.usr_created_by
left outer join {{ ref('r_user') }} usrM
  on usrM.usr_rowid = sCstM.usr_modified_by
left outer join {{ ref('r_user') }} usrD
  on usrD.usr_rowid = sCstM.usr_deleted_by
-- Code Look-ups (latest)
left outer join {{ ref('v_codedefinition') }} cdGender
	on  cdGender.cog_group = 1
	and cdGender.cod_value = sCst.cod_gender
	and cdGender.cod_language = 10
  and cdGender.loadend_ts = to_timestamp('2099-12-31 23:59:59.999', 'yyyy-MM-dd HH:mm:ss.SSS')
left outer join {{ ref('v_codedefinition') }} cdLanguage
	on  cdLanguage.cog_group = 2
	and cdLanguage.cod_value = sCst.cod_language
	and cdLanguage.cod_language = 10
  and cdLanguage.loadend_ts = to_timestamp('2099-12-31 23:59:59.999', 'yyyy-MM-dd HH:mm:ss.SSS')
--where pCst.hk_customer = 'FE69100DB52F4EA1A1982D96E779A8D9E31095F2824D86237EEAE4208DD07369'
--order by pCst.valid_from 