{%- set source_model = "v_cmp_campaign" -%}
{%- set src_pk = "hk_campaign" -%}
{%- set src_nk = "cmp_campaign_bk" -%}
{%- set src_ldts = "load_ts" -%}
{%- set src_source = "record_source" -%}

{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                   src_source=src_source, source_model=source_model) }}