{{ config(materialized='incremental') }}

{%- set source_model = "v_adr_address" -%}
{%- set src_pk = "hk_address_customer" -%}
{%- set src_fk = ["hk_address", "hk_customer"] -%}
{%- set src_ldts = "load_ts" -%}
{%- set src_source = "record_source" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}