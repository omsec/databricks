select
  pCst.hk_customer_snapshot,
  pCst.hk_customer,
  pCst.cst_customer_no,
  pCst.valid_from,
  pCst.valid_to,
  sCstM.cst_created_at,
  sCstM.usr_created_by,
  sCstM.cst_modified_at,
  sCstM.usr_modified_by,
  sCstM.cst_deleted_at,
  sCstM.usr_deleted_by,
  sCst.cod_gender,
  sCst.cst_first_name,
  sCst.cst_last_name,
  sCst.cst_birth_date,
  sCst.cod_language,
  sCst.cst_culture,
  sCst.cst_credit_limit,
  sCstExt.cst_remark,
  sCstExt.cst_attr1,
  sCstExt.cst_attr2,
  sCstExt.cst_attr3,
  sCstExt.cst_attr_int,
  sCstExt.cst_attr_str
from {{ ref('p_customer') }} pCst
join {{ ref('v_customer_meta') }} sCstM
  on  pCst.hk_customer = sCstM.hk_customer
  and pCst.sCstM_load_ts = sCstM.load_ts
join {{ ref('s_customer') }} sCst
  on  sCst.hk_customer = pCst.hk_customer
  and sCst.load_ts = pCst.sCst_load_ts
join {{ ref('v_customer_extended') }} sCstExt
  on  sCstExt.hk_customer = pCst.hk_customer
  and sCstExt.load_ts = pCst.sCstExt_load_ts
--where pCst.cst_customer_no = 'cst26-272.055.321.995.804.7'
--order by pCst.valid_from