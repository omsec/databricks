{%- set yaml_metadata -%}
source_model: 'oit_orderitem'
derived_columns:
  RECORD_SOURCE: '!oit_orderitem'
  LOAD_TS: coalesce(oit_deleted_at, oit_modified_at, oit_created_at) + interval '1 day'
  OIT_ORD_START: coalesce(oit_modified_at, oit_created_at)
  OIT_ORD_END: to_timestamp('2099-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss')
  OIT_ORD_EFFECTIVE_FROM: coalesce(oit_modified_at, oit_created_at)
hashed_columns:
  HK_ORDER_ORDERITEM:
    - 'order_bk'
    - 'product_bk'
    - 'warehouse_bk'
  HK_ORDER: 'order_bk'
  HK_PRODUCT: 'product_bk'
  HK_WAREHOUSE: 'warehouse_bk'
  RH_ORD_OIT:
    is_hashdiff: true
    columns:
      - 'oit_rowid'
      - 'oit_created_at'
      - 'usr_created_by'
      - 'oit_modified_at'
      - 'usr_modified_by'
      - 'oit_deleted_at'
      - 'usr_deleted_by'
      - 'oit_quantity'
      - 'oit_unit_price'
      - 'oit_attr_int'
      - 'oit_attr_str'

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