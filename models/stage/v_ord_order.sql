{%- set yaml_metadata -%}
source_model: 'ord_order'
derived_columns:
  RECORD_SOURCE: '!ord_order'
  LOAD_TS: coalesce(ord_deleted_at, ord_modified_at, ord_created_at) + interval '1 day'
  ORD_CST_SALESMAN_START: coalesce(ord_modified_at, ord_created_at)
  ORD_CST_SALESMAN_END: to_timestamp('2099-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss')
  ORD_ADR_START: coalesce(ord_modified_at, ord_created_at)
  ORD_ADR_END: to_timestamp('2099-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss')
hashed_columns:
  HK_ORDER: 'ord_order_no'
  HK_CUSTOMER: 'customer_bk'
  HK_SALESMAN: 'salesman_bk'
  HK_ADR_DELIVERY: 'adr_delivery_bk'
  HK_ADR_BILLING: 'adr_billing_bk'
  HK_ADR_NOTIFICATION: 'adr_notification_bk'
  HK_ORDER_CUSTOMER_SALESMAN:
    - 'ord_order_no'
    - 'customer_bk'
    - 'salesman_bk'
  HK_ORDER_ADDRESS:
    - 'ord_order_no'
    - 'adr_delivery_bk'
    - 'adr_billing_bk'
    - 'adr_notification_bk'
  RH_ORDER:
    is_hashdiff: true
    columns:
        - 'cod_status'
        - 'ord_sale_ts'
        - 'ord_attr_int'
        - 'ord_attr_str'
  RH_ORDER_META:
    is_hashdiff: true
    columns:
      - 'ord_created_at'
      - 'usr_created_by'
      - 'ord_modified_at'
      - 'usr_modified_by'
      - 'ord_deleted_at'
      - 'usr_deleted_by'
  RH_ORD_CST_SALESMAN:
    is_hashdiff: true
    columns:
      - 'ord_order_no'
      - 'customer_bk'
      - 'salesman_bk'
  RH_ORDER_ADDRESS:
    - 'ord_order_no'
    - 'adr_delivery_bk'
    - 'adr_billing_bk'
    - 'adr_notification_bk'

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