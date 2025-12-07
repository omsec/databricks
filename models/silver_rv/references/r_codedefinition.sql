-- macro only supports single PK
-- if none is available, hash the fields as one (cog_group, cod_value, cod_language)

-- since a reference table is like a hub, it only adds new records, never updates existing ones.
-- hence, materialization is 'table' to build it (full-flush load rather than insert-only)
{{ config(materialized='table') }}

{%- set source_model = 'v_cod_codedefinition' -%}
{%- set src_pk = 'cod_rowid' -%}
{%- set src_extra_columns = ['cog_group', 'cod_value', 'cod_language', 'cod_sort_order', 'cod_default', 'cod_selectable', 'cod_indicator_none', 'cod_indicator_many', 'load_ts', 'record_source'] -%}

{{ automate_dv.ref_table(src_pk=src_pk, 
                         src_extra_columns=src_extra_columns,
                         source_model=source_model) }}