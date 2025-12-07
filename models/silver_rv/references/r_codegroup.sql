-- macro only supports single PK
-- if none is available, hash the fields as one (cog_group, cod_value, cod_language)

-- since a reference table is like a hub, it only adds new records, never updates existing ones.
-- hence, materialization is 'table' to beuild it (full-flush load rather than insert-only)
{{ config(materialized='table') }}

{%- set source_model = 'v_cog_codegroup' -%}
{%- set src_pk = 'cog_rowid' -%}
{%- set src_extra_columns = ['cog_created_at', 'usr_created_by', 'cog_modified_at', 'usr_modified_by', 'cog_deleted_at', 'usr_deleted_by', 'cog_name', 'cog_multi_value', 'load_ts', 'record_source'] -%}

{{ automate_dv.ref_table(src_pk=src_pk,
                         src_extra_columns=src_extra_columns,
                         source_model=source_model) }}