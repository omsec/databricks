{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_adr_address"
src_pk: "HK_ADDRESS_CUSTOMER"
src_hashdiff: 
  source_column: "RH_ADR_CST"
  alias: "ROWHASH"
src_payload:
    - 'HK_ADDRESS'
    - 'HK_CUSTOMER'
    - 'ADR_CST_START'
    - 'ADR_CST_END'
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