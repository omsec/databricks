{{ config(materialized='incremental') }}

{%- set source_model = "v_ord_order" -%}
{%- set src_pk = "hk_order_customer_salesman" -%}
{%- set src_fk = ["hk_order", "hk_customer", "hk_salesman"] -%}
{%- set src_ldts = "load_ts" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}