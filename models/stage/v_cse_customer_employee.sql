{%- set yaml_metadata -%}
source_model: 'cse_customer_employee'
derived_columns:
  RECORD_SOURCE: '!cse_customer_employee'
  LOAD_TS: coalesce(cse_deleted_at, cse_modified_at, cse_created_at) + interval '1 day'
  START_DT: cse_valid_from
  END_DT: cse_valid_to
  EFFECTIVE_FROM: coalesce(cse_modified_at, cse_created_at)
hashed_columns:
  HK_CUSTOMER_EMPLOYEE:
    - 'customer_bk'
    - 'employee_bk'
    - 'cod_role'
  HK_CUSTOMER: 'customer_bk'
  HK_EMPLOYEE: 'employee_bk'
  RH_CUSTOMER_EMPLOYEE:
    is_hashdiff: true
    columns:
      - 'cse_created_at'
      - 'usr_created_by'
      - 'cse_modified_at'
      - 'usr_modified_by'
      - 'cse_deleted_at'
      - 'usr_deleted_by'
      - 'cse_valid_from'
      - 'cse_valid_to'
      - 'cse_primary'

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