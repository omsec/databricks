with ts as (
select
	t.hk_customer_snapshot,
	t.hk_customer,
	t.cst_customer_no,
	t.valid_from,
	coalesce(
		t.cst_deleted_at,
		lead(t.cst_deleted_at) over(partition by t.hk_customer order by t.valid_from),
		lead(t.valid_from) over(partition by t.hk_customer order by t.valid_from) - interval 1 milliseconds,
		to_timestamp('2099-12-31 23:59:59.999', 'yyyy-MM-dd HH:mm:ss.SSS')
	) as valid_to,
	t.cst_created_at,
	t.usr_created_by,
	t.usr_created_by_login_name,
	t.cst_modified_at,
	t.usr_modified_by,
	t.usr_modified_by_login_name,
	lead(t.cst_deleted_at) over(partition by t.hk_customer order by t.valid_from) as cst_deleted_at,
	lead(t.usr_deleted_by) over(partition by t.hk_customer order by t.valid_from) as usr_deleted_by,
	lead(t.usr_deleted_by_login_name) over(partition by t.hk_customer order by t.valid_from) as usr_deleted_by_login_name,
	t.cod_gender,
	t.txt_gender_en,
	t.cst_first_name,
	t.cst_last_name,
	t.cst_birth_date,
	t.cod_language,
	t.txt_language_en,
	t.cst_culture,
	t.cst_credit_limit,
	t.cst_remark,
	t.cst_attr1,
	t.cst_attr2,
	t.cst_attr3,
	t.cst_attr_int,
	t.cst_attr_str,
    t.dwh_applied_issues,
	t.cst_age
from {{ ref('ax_customer') }} t
--where t.hk_customer = 'FE69100DB52F4EA1A1982D96E779A8D9E31095F2824D86237EEAE4208DD07369'
)
select *
from ts
where
	ts.valid_from < ts.valid_to
--order by ts.hk_customer, ts.valid_from