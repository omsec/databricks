{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_adr_address"
src_pk: "hk_address"
src_hashdiff:
  source_column: "RH_ADDRESS"
  alias: "ROWHASH"
src_payload:
    - 'adr_created_at'
    - 'usr_created_by'
    - 'adr_modified_at'
    - 'usr_modified_by'
    - 'adr_deleted_at'
    - 'usr_deleted_by'
    - 'cod_type'
    - 'adr_priority'
    - 'adr_address'
    - 'adr_primary'
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