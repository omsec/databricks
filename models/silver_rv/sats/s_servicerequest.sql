{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_srv_servicerequest"
src_pk: "HK_SERVICEREQUEST"
src_hashdiff: 
  source_column: "RH_SERVICEREQUEST"
  alias: "HASHDIFF"
src_payload:
    - 'srv_created_at'
    - 'usr_created_by'
    - 'srv_modified_at'
    - 'usr_modified_by'
    - 'srv_deleted_at'
    - 'usr_deleted_by'
    - 'srv_subject'
    - 'srv_message'
    - 'cod_priority'
    - 'cod_status'
    - 'srv_attr_int'
    - 'srv_attr_str'
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