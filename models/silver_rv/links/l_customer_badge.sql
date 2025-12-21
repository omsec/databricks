{{ config(materialized='incremental') }}

{%- set source_model = "v_csb_customer_badge" -%}
{%- set src_pk = "hk_customer_badge" -%}
{%- set src_fk = ["hk_customer", "hk_badge"] -%}
{%- set src_ldts = "load_ts" -%}
{%- set src_source = "record_source" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}