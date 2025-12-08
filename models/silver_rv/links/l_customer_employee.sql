{{ config(materialized='incremental') }}

{%- set source_model = "v_cse_customer_employee" -%}
{%- set src_pk = "HK_CUSTOMER_EMPLOYEE" -%}
{%- set src_fk = ["hk_customer", "hk_employee", "cod_role"] -%}
{%- set src_ldts = "load_ts" -%}
{%- set src_source = "record_source" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}