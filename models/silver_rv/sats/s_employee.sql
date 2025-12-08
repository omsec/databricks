{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_emp_employee"
src_pk: "HK_EMPLOYEE"
src_hashdiff:
  source_column: "RH_EMPLOYEE"
  alias: "ROWHASH"
src_payload:
  - 'emp_rowid'
  - 'emp_created_at'
  - 'usr_created_by'
  - 'emp_modified_at'
  - 'usr_modified_by'
  - 'emp_deleted_at'
  - 'usr_deleted_by'
  - 'emp_first_name'
  - 'emp_last_name'
  - 'emp_email'
  - 'emp_phone'
  - 'emp_birth_date'
  - 'emp_hire_date'
  - 'emp_job_title'
  - 'usr_account_id'
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