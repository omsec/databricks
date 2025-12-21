{%- set source_model = "v_bdg_badge" -%}
{%- set src_pk = "hk_badge" -%}
{%- set src_nk = "bdg_badge_bk" -%}
{%- set src_ldts = "load_ts" -%}
{%- set src_source = "record_source" -%}

{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                   src_source=src_source, source_model=source_model) }}