{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_cmp_campaign"
src_pk: "HK_CAMPAIGN"
src_hashdiff:
  source_column: "CAMPAIGN_HASHDIFF"
  alias: "HASHDIFF"
src_payload:
    - 'cmp_created_at'
    - 'usr_created_by'
    - 'cmp_modified_at'
    - 'usr_modified_by'
    - 'cmp_deleted_at'
    - 'usr_deleted_by'
    - 'cmp_channel'
    - 'cmp_description'
src_ldts: "load_ts"
src_source: "record_source"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(src_pk=metadata_dict["src_pk"],
                   src_hashdiff=metadata_dict["src_hashdiff"],
                   src_payload=metadata_dict["src_payload"],
                   src_eff=metadata_dict["src_eff"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"])   }}