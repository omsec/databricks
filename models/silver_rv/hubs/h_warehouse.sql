{%- set source_model = "v_wrh_warehouse" -%}
{%- set src_pk = "hk_warehouse" -%}
{%- set src_nk = "warehouse_bk" -%}
{%- set src_ldts = "load_ts" -%}
{%- set src_source = "record_source" -%}

{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                   src_source=src_source, source_model=source_model) }}