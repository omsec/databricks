{{ config(materialized='incremental') }}

{%- set source_model = "v_ord_order" -%}
{%- set src_pk = "hk_order_address" -%}
{%- set src_fk = ["hk_order", "hk_adr_delivery", "hk_adr_billing", "hk_adr_notification"] -%}
{%- set src_ldts = "load_ts" -%}
{%- set src_source = "record_source" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}