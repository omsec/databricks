{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_cst_customer"
src_pk: "HK_CUSTOMER"
src_hashdiff: 
  source_column: "CUSTOMER_HASHDIFF"
  alias: "HASHDIFF"
src_payload:
  - "cod_gender"
  - "cst_first_name"
  - "cst_last_name"
  - "cst_birth_date"
  - "cod_language"
  - "cst_culture"
  - "cst_credit_limit"
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