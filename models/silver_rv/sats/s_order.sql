{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_ord_order"
src_pk: "HK_ORDER"
src_hashdiff:
  source_column: "RH_ORDER"
  alias: "HASHDIFF"
src_payload:
    - 'ord_created_at'
    - 'usr_created_by'
    - 'ord_modified_at'
    - 'usr_modified_by'
    - 'ord_deleted_at'
    - 'usr_deleted_by'
    - 'cod_status'
    - 'ord_sale_ts'
    - 'ord_attr_int'
    - 'ord_attr_str'
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