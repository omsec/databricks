{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_pct_productcategory"
src_pk: "HK_PRODUCTCATEGORY"
src_hashdiff: 
  source_column: "RH_PRODUCTCATEGORY"
  alias: "HASHDIFF"
src_payload:
    - 'pct_created_at'
    - 'usr_created_by'
    - 'pct_modified_at'
    - 'usr_modified_by'
    - 'pct_deleted_at'
    - 'usr_deleted_by'
    - 'pct_name'
    - 'pct_import_filename'
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