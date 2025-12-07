{%- set yaml_metadata -%}
source_model: 'usr_user'
derived_columns:
  RECORD_SOURCE: '!USR_USER'
  LOAD_TS: coalesce(usr_deleted_at, usr_modified_at, usr_created_at) + interval '1 day'
hashed_columns:
  USER_HK: 'usr_login_name'
  USER_HASHDIFF:
    is_hashdiff: true
    columns:
        - 'usr_created_at'
        - 'usr_created_by'
        - 'usr_modified_at'
        - 'usr_modified_by'
        - 'usr_deleted_at'
        - 'usr_deleted_by'
        - 'usr_active'
        - 'usr_full_name'
        - 'usr_last_login'
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