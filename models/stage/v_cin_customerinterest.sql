-- we're forced to add a column named EFFECTIVE_FROM :-/
{%- set yaml_metadata -%}
source_model: 'cin_customerinterest'
derived_columns:
  RECORD_SOURCE: '!cin_customerinterest'
  LOAD_TS: coalesce(cin_deleted_at, cin_modified_at, cin_created_at) + interval '1 day'
  EFFECTIVE_FROM: coalesce(cin_modified_at, cin_created_at)
hashed_columns:
  HK_CUSTOMER: 'customer_bk'
  ROWHASH:
    is_hashdiff: true
    columns:
        - 'cin_created_at'
        - 'usr_created_by'
        - 'cin_modified_at'
        - 'usr_modified_by'
        - 'cin_deleted_at'
        - 'usr_deleted_by'

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