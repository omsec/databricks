{%- set yaml_metadata -%}
source_model: 'cog_codegroup'
derived_columns:
  RECORD_SOURCE: '!cog_codegroup'
  LOAD_TS: coalesce(cog_deleted_at, cog_modified_at, cog_created_at) + interval '1 day'

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