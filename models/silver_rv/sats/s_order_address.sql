{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_ord_order"
src_pk: "hk_order_address"
src_hashdiff:
  source_column: "rh_order_address"
  alias: "rowhash"
src_payload:
    - 'HK_ORDER'
    - 'HK_ADR_DELIVERY'
    - 'HK_ADR_BILLING'
    - 'HK_ADR_NOTIFICATION'
    - 'ORD_ADR_START'
    - 'ORD_ADR_END'
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