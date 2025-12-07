-- cmp start/end added b/c of incorrect sat loading
{%- set yaml_metadata -%}
source_model: 'cst_customer'
derived_columns:
  RECORD_SOURCE: '!cst_customer'
  LOAD_TS: coalesce(cst_deleted_at, cst_modified_at, cst_created_at) + interval '1 day'
  CST_CMP_START: coalesce(cst_modified_at, cst_created_at)
  CST_CMP_END: to_timestamp('2099-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss')
hashed_columns:
  HK_CUSTOMER: 'cst_customer_no'
  HK_CAMPAIGN: 'cmp_campaign_bk'
  HK_CUSTOMER_CAMPAIGN:
    - 'cst_customer_no'
    - 'cmp_campaign_bk'
  CUSTOMER_HASHDIFF:
    is_hashdiff: true
    columns:
        - 'cod_gender'
        - 'cst_first_name'
        - 'cst_last_name'
        - 'cst_birth_date'
        - 'cod_language'
        - 'cst_culture'
        - 'cst_credit_limit'
  CUSTOMER_META_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'cst_created_at'
      - 'usr_created_by'
      - 'cst_modified_at'
      - 'usr_modified_by'
      - 'cst_deleted_at'
      - 'usr_deleted_by'
  CUSTOMER_EXTENDED_HASHDIFF:
    is_hashdiff: true
    columns:
        - 'cst_remark'
        - 'cst_attr1'
        - 'cst_attr2'
        - 'cst_attr3'
        - 'cst_attr_int'
        - 'cst_attr_str'
  CUSTOMER_CAMPAIGN_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'cmp_campaign_bk'

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