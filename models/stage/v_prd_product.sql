{%- set yaml_metadata -%}
source_model: 'prd_product'
derived_columns:
  RECORD_SOURCE: '!PRD_PRODUCT'
  LOAD_TS: coalesce(prd_deleted_at, prd_modified_at, prd_created_at) + interval '1 day'
  PRD_PCT_START: coalesce(prd_modified_at, prd_created_at)
  PRD_PCT_END: to_timestamp('2099-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss')
hashed_columns:
  HK_PRODUCT: 'product_bk'
  HK_PRODUCTCATEGORY: 'productcategory_bk'
  HK_PRODUCT_PRODUCTCATEGORY:
    - 'product_bk'
    - 'productcategory_bk'
  RH_PRODUCT:
    is_hashdiff: true
    columns:
      - 'prd_created_at'
      - 'usr_created_by'
      - 'prd_modified_at'
      - 'usr_modified_by'
      - 'prd_deleted_at'
      - 'usr_deleted_by'
      - 'prd_name'
      - 'prd_description'
      - 'prd_standard_cost'
      - 'prd_list_price'
      - 'prd_sold_until'
  RH_PRODUCT_PRODUCTCATEGORY:
    is_hashdiff: true
    columns:
      - 'product_bk'
      - 'productcategory_bk'

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