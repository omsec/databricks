{%- set yaml_metadata -%}
source_model: 'wrh_warehouse'
derived_columns:
  RECORD_SOURCE: '!wrh_warehouse'
  LOAD_TS: coalesce(wrh_deleted_at, wrh_modified_at, wrh_created_at) + interval '1 day'
hashed_columns:
  HK_WAREHOUSE: 'warehouse_bk'
  RH_WAREHOUSE:
    is_hashdiff: true
    columns:
        - 'wrh_created_at'
        - 'usr_created_by'
        - 'wrh_modified_at'
        - 'usr_modified_by'
        - 'wrh_deleted_at'
        - 'usr_deleted_by'
        - 'wrh_name'
        - 'wrh_capacity'

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