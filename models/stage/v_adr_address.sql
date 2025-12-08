{%- set yaml_metadata -%}
source_model: 'adr_address'
derived_columns:
  RECORD_SOURCE: '!adr_address'
  LOAD_TS: coalesce(adr_deleted_at, adr_modified_at, adr_created_at) + interval '1 day'
  ADR_CST_START: coalesce(adr_modified_at, adr_created_at)
  ADR_CST_END: to_timestamp('2099-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss')
hashed_columns:
  HK_ADDRESS: 'adr_rowid'
  HK_CUSTOMER: 'customer_bk'
  HK_ADDRESS_CUSTOMER:
    - 'adr_rowid'
    - 'customer_bk'
  RH_ADDRESS:
    is_hashdiff: true
    columns:
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
  RH_ADR_CST:
    is_hashdiff: true
    columns:
      - 'adr_rowid'
      - 'customer_bk'

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