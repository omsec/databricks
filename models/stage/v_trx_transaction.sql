{%- set yaml_metadata -%}
source_model: 'trx_transaction'
derived_columns:
  RECORD_SOURCE: '!trx_transaction'
  LOAD_TS: trx_created_at + interval '1 day'
  EFFECTIVE_FROM: trx_created_at
hashed_columns:
  HK_TRANSACTION: 'trx_transaction_no'
  HK_ORDER: 'order_bk'
  HK_CUSTOMER: 'customer_bk'
  HK_EMPLOYEE: 'employee_bk'
  RH_TRX:
    is_hashdiff: true
    columns:
        - 'trx_created_at'
        - 'usr_created_by'
        - 'trx_cst_customer_no'
        - 'trx_ord_order_no'
        - 'trx_created_by'
        - 'trx_cst_first_name'
        - 'trx_cst_last_name'
        - 'trx_emp_first_name'
        - 'trx_emp_last_name'
        - 'trx_amount'
        - 'trx_method'

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=source_model,
                     derived_columns=derived_columns,
                     null_columns=none,
                     hashed_columns=hashed_columns,
                     ranked_columns=none) }}