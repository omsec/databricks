{{ config(materialized='incremental') }}

{%- set source_model = "v_srv_servicerequest" -%}
{%- set src_pk = "HK_SERVICEREQUEST_CUSTOMER" -%}
{%- set src_fk = ["HK_SERVICEREQUEST", "HK_CUSTOMER"] -%}
{%- set src_ldts = "load_ts" -%}
{%- set src_source = "record_source" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}