-- SCD2 by design (incremental)

{{
    config(
        materialized='incremental',
        unique_key='d_customer'
    )
}}

-- NOTE:
-- last version of COD/USR is applied by the BV-views (fe) for all states

select
	cst.hk_customer_snapshot as d_customer,
	cst.hk_customer,
	cst.cst_customer_no,
	cst.valid_from,
	cst.valid_to,
	cst.cst_first_name,
	cst.cst_last_name,
	cst.txt_gender_en,
	cst.txt_language_en,
	--cst.cst_birth_date, -- redacted for data protection
	cst.cst_age,
	cst.cst_credit_limit,
	cst.cst_remark,
	cst.cst_attr1,
	cst.cst_attr2,
	cst.cst_attr3
from {{ ref('fh_customer') }} cst
order by
	cst.valid_from,
	cst.cst_customer_no