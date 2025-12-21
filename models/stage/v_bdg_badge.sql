{%- set yaml_metadata -%}
source_model: 'bdg_badge'
derived_columns:
  record_source: '!bdg_badge'
  load_ts: coalesce(bdg_deleted_at, bdg_modified_at, bdg_created_at) + interval '1 day'
hashed_columns:
  hk_badge: 'bdg_badge_bk'
  rh_badge:
    is_hashdiff: true
    columns:
        - 'bdg_created_at'
        - 'usr_created_by'
        - 'bdg_modified_at'
        - 'usr_modified_by'
        - 'bdg_deleted_at'
        - 'usr_deleted_by'
        - 'bdg_name'
        - 'bdg_type1'
        - 'bdg_type2'
        - 'bdg_abilities'
        - 'bdg_generation'
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