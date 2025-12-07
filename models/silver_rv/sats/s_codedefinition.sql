{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_cod_codedefinition"
src_pk:
    - "cog_group"
    - "cod_value"
    - "cod_language"

src_hashdiff:
  source_column: "CODEDEFINITION_HASHDIFF"
  alias: "rowhash"
src_payload:
    - 'cod_created_at'
    - 'usr_created_by'
    - 'cod_modified_at'
    - 'usr_modified_by'
    - 'cod_deleted_at'
    - 'usr_deleted_by'
    - "cod_text"
    - "cod_short"

src_ldts: "load_ts"
src_source: "RECORD_SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(src_pk=metadata_dict["src_pk"],
                   src_hashdiff=metadata_dict["src_hashdiff"],
                   src_payload=metadata_dict["src_payload"],
                   src_eff=metadata_dict["src_eff"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"])   }}