{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_prd_product"
src_pk: "HK_PRODUCT"
src_hashdiff: 
  source_column: "RH_PRODUCT"
  alias: "HASHDIFF"
src_payload:
    - 'prd_created_at'
    - 'usr_created_by'
    - 'prd_modified_at'
    - 'usr_modified_by'
    - 'prd_deleted_at'
    - 'usr_deleted_by'
    - 'prd_name'
    - 'prd_description'
    - 'prd_standard_cost'
    - 'prd_list_price'
    - 'prd_sold_until'
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