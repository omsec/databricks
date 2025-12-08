{{ config(materialized='incremental')  }}

{%- set source_model = "v_srv_servicerequest" -%}
{%- set src_pk = "HK_SERVICEREQUEST_CUSTOMER" -%}
{%- set src_dfk = "HK_SERVICEREQUEST" -%}
{%- set src_sfk = "HK_CUSTOMER" -%}
{%- set src_start_date = "SRV_CST_START" -%}
{%- set src_end_date = "SRV_CST_END" -%}

{%- set src_eff = "SRV_CST_EFFECTIVE_FROM" -%}
{%- set src_ldts = "load_ts" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.eff_sat(src_pk=src_pk, src_dfk=src_dfk, src_sfk=src_sfk,
                       src_start_date=src_start_date, 
                       src_end_date=src_end_date,
                       src_eff=src_eff, src_ldts=src_ldts, 
                       src_source=src_source,
                       source_model=source_model) }}