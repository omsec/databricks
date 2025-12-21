{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_csb_customer_badge"
src_pk: "hk_customer_badge"
src_hashdiff:
  source_column: "rh_cst_bdg"
  alias: "rowhash"
src_payload:
    - 'hk_customer'
    - 'hk_badge'
    - 'cst_bdg_start'
    - 'cst_bdg_end'
    - 'csb_rowid'
    - 'csb_created_at'
    - 'usr_created_by'
    - 'csb_modified_at'
    - 'usr_modified_by'
    - 'csb_deleted_at'
    - 'usr_deleted_by'
    - 'cst_customer'
    - 'bdg_badge'
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