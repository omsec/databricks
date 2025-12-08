{%- set yaml_metadata -%}
source_model: 'emp_employee'
derived_columns:
  RECORD_SOURCE: '!emp_employee'
  LOAD_TS: coalesce(emp_deleted_at, emp_modified_at, emp_created_at) + interval '1 day'
hashed_columns:
  HK_EMPLOYEE: 'emp_employee_no'
  HK_MANAGER: 'manager_bk'
  RH_EMPLOYEE:
    is_hashdiff: true
    columns:
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
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=source_model,
                     derived_columns=derived_columns,
                     null_columns=none,
                     hashed_columns=hashed_columns,
                     ranked_columns=none) }}