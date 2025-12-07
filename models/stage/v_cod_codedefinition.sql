{%- set yaml_metadata -%}
source_model: 'cod_codedefinition'
derived_columns:
  RECORD_SOURCE: '!cod_codedefinition'
  LOAD_TS: coalesce(cod_deleted_at, cod_modified_at, cod_created_at) + interval '1 day'
hashed_columns:
  CODEDEFINITION_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'cod_created_at'
      - 'usr_created_by'
      - 'cod_modified_at'
      - 'usr_modified_by'
      - 'cod_deleted_at'
      - 'usr_deleted_by'
      - 'cod_text'
      - 'cod_short'

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=source_model,
                     derived_columns=derived_columns,
                     null_columns=none,
                     hashed_columns=hashed_columns,
                     ranked_columns=none) }}