{%- set yaml_metadata -%}
source_model: 'exg_expertsgroup'
derived_columns:
  RECORD_SOURCE: '!exg_expertsgroup'
  LOAD_TS: coalesce(exg_deleted_at, exg_modified_at, exg_created_at) + interval '1 day'
  EXG_MGR_START: coalesce(exg_modified_at, exg_created_at)
  EXG_MGR_END: to_timestamp('2099-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss')
hashed_columns:
  HK_EXPERTSGROUP: 'exg_rowid'
  HK_MANAGER: 'manager_bk'
  HK_EXPERTSGROUP_MANAGER:
    - 'exg_group_bk'
    - 'manager_bk'
  RH_GROUP:
    is_hashdiff: true
    columns:
        - 'exg_created_at'
        - 'usr_created_by'
        - 'exg_modified_at'
        - 'usr_modified_by'
        - 'exg_deleted_at'
        - 'usr_deleted_by'
        - 'exg_name'
        - 'exg_description'
        - 'exg_active'
  RH_GROUP_MANAGER:
    - 'manager_bk'

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