with rawtable as (
	select
		pct.*
	from {{ source('bronze_lz', 'pct_productcategory')}} pct
    where
         coalesce(pct.pct_deleted_at, pct.pct_modified_at, pct.pct_created_at) > to_date('{{ var('load_date') }}', 'yyyy-MM-dd')
)
select
	t.pct_rowid,
	t.pct_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	t.pct_modified_at,
	cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
	t.pct_deleted_at,
	cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
	-- payload
	t.pct_name,
	t.pct_import_filename,
    -- derrived
    cast(t.pct_rowid as varchar(20)) as productcategory_bk
from rawtable t