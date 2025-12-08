{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_ord_order"
src_pk: "HK_ORDER_CUSTOMER_SALESMAN"
src_hashdiff: 
  source_column: "RH_ORD_CST_SALESMAN"
  alias: "ROWHASH"
src_payload:
    - 'HK_ORDER'
    - 'HK_CUSTOMER'
    - 'HK_SALESMAN'
    - 'ORD_CST_SALESMAN_START'
    - 'ORD_CST_SALESMAN_END'
src_ldts: "load_ts"
src_source: "record_source"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(src_pk=metadata_dict["src_pk"],
                   src_hashdiff=metadata_dict["src_hashdiff"],
                   src_payload=metadata_dict["src_payload"],
                   src_eff=metadata_dict["src_eff"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"])   }}