{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_oit_orderitem"
src_pk: "HK_ORDER_ORDERITEM"
src_hashdiff:
  source_column: "RH_ORD_OIT"
  alias: "ROWHASH"
src_payload:
    - 'oit_rowid'
    - 'oit_created_at'
    - 'usr_created_by'
    - 'oit_modified_at'
    - 'usr_modified_by'
    - 'oit_deleted_at'
    - 'usr_deleted_by'
    - 'oit_quantity'
    - 'oit_unit_price'
    - 'oit_attr_int'
    - 'oit_attr_str'
    - 'OIT_ORD_START'
    - 'OIT_ORD_END'
src_eff: "OIT_ORD_EFFECTIVE_FROM"
src_ldts: "LOAD_TS"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(src_pk=metadata_dict["src_pk"],
                   src_hashdiff=metadata_dict["src_hashdiff"],
                   src_payload=metadata_dict["src_payload"],
                   src_eff=metadata_dict["src_eff"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"])   }}