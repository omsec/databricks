-- currently set to 'table' since 'incremental' adds all records at each run - bug?
{{ config(materialized='table') }}

{%- set yaml_metadata -%}
source_model: 'v_cin_customerinterest'
src_pk: 'HK_CUSTOMER'
src_cdk:
  - 'cin_info'
src_payload:
  - 'cin_rowid'
  - 'cin_created_at'
  - 'usr_created_by'
  - 'cin_modified_at'
  - 'usr_modified_by'
  - 'cin_deleted_at'
  - 'usr_deleted_by'
src_hashdiff: 'ROWHASH'
src_eff: 'EFFECTIVE_FROM'
src_ldts: 'LOAD_TS'
src_source: 'RECORD_SOURCE'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.ma_sat(src_pk=metadata_dict['src_pk'],
                      src_cdk=metadata_dict['src_cdk'],
                      src_payload=metadata_dict['src_payload'],
                      src_hashdiff=metadata_dict['src_hashdiff'],
                      src_eff=metadata_dict['src_eff'],
                      src_ldts=metadata_dict['src_ldts'],
                      src_source=metadata_dict['src_source'],
                      source_model=metadata_dict['source_model']) }}