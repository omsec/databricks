{%- set yaml_metadata -%}
source_model: 'cxg_customer_expertsgroup'
derived_columns:
  RECORD_SOURCE: '!cxg_customer_expertsgroup'
  LOAD_TS: coalesce(cxg_deleted_at, cxg_modified_at, cxg_created_at) + interval '1 day'
  cxg_MGR_START: coalesce(cxg_modified_at, cxg_created_at)
  cxg_MGR_END: to_timestamp('2099-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss')
hashed_columns:
  HK_CUSTOMER: 'cst_customer_bk'
  HK_EXPERTSGROUP: 'exg_group_bk'
  HK_CUSTOMER_EXPERTSGROUP:
    - 'cst_customer_bk'
    - 'exg_group_bk'
  RH_CUSTOMER_GROUP:
    is_hashdiff: true
    columns:
        - 'cxg_created_at'
        - 'usr_created_by'
        - 'cxg_modified_at'
        - 'usr_modified_by'
        - 'cxg_deleted_at'
        - 'usr_deleted_by'
        - 'cxg_effective_from'
        - 'cxg_effective_to'
        - 'cxg_level'
        - 'cxg_moderator'

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