{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_cse_customer_employee"
src_pk: "HK_CUSTOMER_EMPLOYEE"
src_hashdiff: 
  source_column: "RH_CUSTOMER_EMPLOYEE"
  alias: "ROWHASH"
src_payload:
    - 'cse_created_at'
    - 'usr_created_by'
    - 'cse_modified_at'
    - 'usr_modified_by'
    - 'cse_deleted_at'
    - 'usr_deleted_by'
    - 'cod_role'
    - 'cse_valid_from'
    - 'cse_valid_to'
    - 'cse_primary'
src_eff: EFFECTIVE_FROM    
src_ldts: "LOAD_TS"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

-- using standard satellite macro instead of eff_sat,
-- since the eff_sat macro ignores any changes in payload

{{ automate_dv.sat(src_pk=metadata_dict["src_pk"],
                   src_hashdiff=metadata_dict["src_hashdiff"],
                   src_payload=metadata_dict["src_payload"],
                   src_eff=metadata_dict["src_eff"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"])   }}