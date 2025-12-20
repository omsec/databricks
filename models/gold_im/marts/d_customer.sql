-- SCD2 by design (incremental)

{{
    config(
        materialized='incremental',
        unique_key='d_customer'
    )
}}

-- NOTE:
-- last version of COD is applied by the VS-views for all states

select
	pCst.hk_customer_snapshot as d_customer,
	pCst.hk_customer,
	pCst.cst_customer_no,
	pCst.valid_from,
	pCst.valid_to,
	sCst.cst_first_name,
	sCst.cst_last_name,
	sCst.txt_gender_en,
	sCst.txt_language_en,
	--sCst.cst_birth_date, -- redacted for data protection
	sCst.cst_age,
	sCst.cst_credit_limit,
	sCstExt.cst_remark,
	sCstExt.cst_attr1,
	sCstExt.cst_attr2,
	sCstExt.cst_attr3
from {{ ref('p_customer') }} pCst
join {{ ref('vs_customer') }} sCst
	on  sCst.hk_customer = pCst.hk_customer
	and sCst.load_ts = pCst.scst_load_ts
join {{ ref('vs_customer_extended') }} sCstExt
	on  sCstExt.hk_customer = pCst.hk_customer
	and sCstExt.load_ts = pCst.scstext_load_ts
order by
	pCst.valid_from,
	pCst.cst_customer_no