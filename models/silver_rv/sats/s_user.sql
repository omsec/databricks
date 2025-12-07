{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "v_usr_user"
src_pk:
    - "usr_rowid"
src_hashdiff:
  source_column: "USER_HASHDIFF"
  alias: "HASHDIFF"
src_payload:
  - "usr_created_at"
  - "usr_created_by"
  - "usr_modified_at"
  - "usr_modified_by"
  - "usr_deleted_at"
  - "usr_deleted_by"
  - "usr_active"
  - "usr_full_name"
  - "usr_last_login"
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