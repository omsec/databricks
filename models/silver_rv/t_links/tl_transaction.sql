{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: 'v_trx_transaction'
src_pk: 'HK_TRANSACTION'
src_fk: 
    - 'hk_order'
    - 'hk_customer'
    - 'hk_employee'
src_payload:
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
src_eff: 'EFFECTIVE_FROM'
src_ldts: 'load_ts'
src_source: 'RECORD_SOURCE'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.t_link(src_pk=metadata_dict["src_pk"],
                      src_fk=metadata_dict["src_fk"],
                      src_payload=metadata_dict["src_payload"],
                      src_eff=metadata_dict["src_eff"],
                      src_ldts=metadata_dict["src_ldts"],
                      src_source=metadata_dict["src_source"],
                      source_model=metadata_dict["source_model"]) }}