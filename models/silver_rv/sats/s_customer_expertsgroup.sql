{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_cxg_customer_expertsgroup"
src_pk: "hk_customer_expertsgroup"
src_hashdiff:
  source_column: "rh_customer_group"
  alias: "rowhash"
src_payload:
    - 'hk_customer'
    - 'hk_expertsgroup'
    - 'cxg_rowid'
    - 'cxg_created_at'
    - 'usr_created_by'
    - 'cxg_modified_at'
    - 'usr_modified_by'
    - 'cxg_deleted_at'
    - 'usr_deleted_by'
    - 'cxg_effective_from'
    - 'cxg_effective_to'
    - 'cxg_level'
    - 'cxg_moderator'
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