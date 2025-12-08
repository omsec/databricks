{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_exg_expertsgroup"
src_pk: "HK_EXPERTSGROUP"
src_hashdiff:
  source_column: "RH_GROUP"
  alias: "ROWHASH"
src_payload:
    - 'exg_rowid'
    - 'exg_created_at'
    - 'usr_created_by'
    - 'exg_modified_at'
    - 'usr_modified_by'
    - 'exg_deleted_at'
    - 'usr_deleted_by'
    - 'exg_name'
    - 'exg_description'
    - 'exg_active'

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