{{ config(materialized='incremental') }}

{%- set source_model = "v_oit_orderitem" -%}
{%- set src_pk = "HK_ORDER_ORDERITEM" -%}
{%- set src_fk = ["hk_order", "hk_product", "hk_warehouse"] -%}
{%- set src_ldts = "load_ts" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}