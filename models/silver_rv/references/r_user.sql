-- macro only supports single PK
-- if none is available, hash the fields as one (cog_group, cod_value, cod_language)

-- since a reference table is like a hub, it only adds new records, never updates existing ones.
-- hence, materialization is 'table' to beuild it (full-flush load rather than insert-only)

-- INFO:
-- perhaps it's usful to include usr_full_name here, if it's used frequently
-- and usr_last_login since this might create lots of history-records
{{ config(materialized='table') }}

{%- set source_model = 'v_usr_user' -%}
{%- set src_pk = 'usr_rowid' -%}
{%- set src_extra_columns = ['usr_login_name', 'load_ts', 'record_source'] -%}

{{ automate_dv.ref_table(src_pk=src_pk, 
                         src_extra_columns=src_extra_columns,
                         source_model=source_model) }}