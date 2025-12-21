{%- set yaml_metadata -%}
source_model: 'csb_customer_badge'
derived_columns:
  record_source: '!csb_customer_badge'
  load_ts: coalesce(csb_deleted_at, csb_modified_at, csb_created_at) + interval '1 day'
  cst_bdg_start: coalesce(csb_modified_at, csb_created_at)
  cst_bdg_end: to_timestamp('2099-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss')
hashed_columns:
  hk_customer: 'cst_customer_bk'
  hk_badge: 'bdg_badge_bk'
  hk_customer_badge:
    - 'cst_customer_bk'
    - 'bdg_badge_bk'
  rh_csb:
    is_hashdiff: true
    columns:
        - 'csb_created_at'
        - 'usr_created_by'
        - 'csb_modified_at'
        - 'usr_modified_by'
        - 'csb_deleted_at'
        - 'usr_deleted_by'
  rh_cst_bdg:
    - 'cst_customer'
    - 'bdg_badge'

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