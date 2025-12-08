{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_exg_expertsgroup"
src_pk: "HK_EXPERTSGROUP_MANAGER"
src_hashdiff:
  source_column: "RH_GROUP_MANAGER"
  alias: "ROWHASH"
src_payload:
    - 'HK_EXPERTSGROUP'
    - 'HK_MANAGER'
    - 'EXG_MGR_START'
    - 'EXG_MGR_END'
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