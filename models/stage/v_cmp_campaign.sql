{%- set yaml_metadata -%}
source_model: 'cmp_campaign'
derived_columns:
  RECORD_SOURCE: '!CMP_CAMPAIGN'
  LOAD_TS: coalesce(cmp_deleted_at, cmp_modified_at, cmp_created_at) + interval '1 day'
hashed_columns:
  HK_CAMPAIGN: 'cmp_campaign_bk'
  CAMPAIGN_HASHDIFF:
    is_hashdiff: true
    columns:
        - 'cmp_created_at'
        - 'usr_created_by'
        - 'cmp_modified_at'
        - 'usr_modified_by'
        - 'cmp_deleted_at'
        - 'usr_deleted_by'
        - 'cmp_channel'
        - 'cmp_description'

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