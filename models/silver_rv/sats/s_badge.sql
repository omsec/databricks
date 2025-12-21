{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_bdg_badge"
src_pk: "hk_badge"
src_hashdiff:
  source_column: "rh_badge"
  alias: "rowhash"
src_payload:
    - 'bdg_rowid'
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