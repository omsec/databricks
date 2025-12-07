-- Hard Rules go here, eg. Type-Casting
with rawtable as (
	select *
	from {{ source('bronze_lz', 'cog_codegroup')}}
)
select
	t.cog_rowid,
	t.cog_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	t.cog_modified_at,
	cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
	t.cog_deleted_at,
	cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
	-- payload
	t.cog_name,
	t.cog_multi_value
from rawtable t
-- cog & cog are full-flush loads (dbt materialization 'table')
-- hence, we don't need any delta check here