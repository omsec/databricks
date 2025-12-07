-- Hard Rules go here, eg. Type-Casting
with rawtable as (
	select *
	from {{ source('bronze_lz', 'cod_codedefinition')}}
)
select
	t.cod_rowid,
	t.cod_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	t.cod_modified_at,
	cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
	t.cod_deleted_at,
	cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
	-- payload
	t.cog_group,
    -- this avoids run-time casting, eg. in coalesce-statements
	cast(t.cod_value as varchar(20)) as cod_value,
	--t.cod_value,
	t.cod_language,
	t.cod_text,
	t.cod_short,
	t.cod_sort_order,
	t.cod_default,
	t.cod_selectable,
	t.cod_indicator_none,
	t.cod_indicator_many
from rawtable t
-- cog & cod are full-flush loads (dbt materialization 'table')
-- hence, we don't need any delta check here