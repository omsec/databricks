{%- set yaml_metadata -%}
source_model: 'srv_servicerequest'
derived_columns:
  RECORD_SOURCE: '!srv_servicerequest'
  LOAD_TS: coalesce(srv_deleted_at, srv_modified_at, srv_created_at) + interval '1 day'
  SRV_CST_START: coalesce(srv_modified_at, srv_created_at)
  SRV_CST_END: to_timestamp('2099-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss')
  SRV_CST_EFFECTIVE_FROM: coalesce(srv_modified_at, srv_created_at)
hashed_columns:
  HK_SERVICEREQUEST: 'srv_request_no'
  HK_CUSTOMER: 'customer_bk'
  HK_SERVICEREQUEST_CUSTOMER:
    - 'srv_request_no'
    - 'customer_bk'
  RH_SERVICEREQUEST:
    is_hashdiff: true
    columns:
        - 'srv_created_at'
        - 'usr_created_by'
        - 'srv_modified_at'
        - 'usr_modified_by'
        - 'srv_deleted_at'
        - 'usr_deleted_by'
        - 'srv_subject'
        - 'srv_message'
        - 'cod_priority'
        - 'cod_status'
        - 'srv_attr_int'
        - 'srv_attr_str'

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