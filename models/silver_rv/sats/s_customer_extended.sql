{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_cst_customer"
src_pk: "HK_CUSTOMER"
src_hashdiff: 
  source_column: "CUSTOMER_EXTENDED_HASHDIFF"
  alias: "HASHDIFF"
src_payload:
  - "cst_remark"
  - "cst_attr1"
  - "cst_attr2"
  - "cst_attr3"
  - "cst_attr_int"
  - "cst_attr_str"
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