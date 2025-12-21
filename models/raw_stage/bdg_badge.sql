with rawtable as (
	select
		t.*
	from {{ source('bronze_lz', 'bdg_badge')}} t
	where
		coalesce(t.bdg_deleted_at, t.bdg_modified_at, t.bdg_created_at) > to_date('{{ var('load_date') }}', 'yyyy-MM-dd')
)
select
	t.bdg_rowid,
	t.bdg_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	t.bdg_modified_at,
	cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
	t.bdg_deleted_at,
	cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
	-- payload
	t.bdg_name,
    t.bdg_type1,
    t.bdg_type2,
    t.bdg_abilities,
    t.bdg_generation,
	-- derrived
	cast(t.bdg_rowid as varchar(20)) as bdg_badge_bk
from rawtable t