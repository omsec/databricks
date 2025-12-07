with rawtable as (
	select
        cmp.*
	from {{ source('bronze_lz', 'cmp_campaign')}} cmp
    where
        coalesce(cmp.cmp_deleted_at, cmp.cmp_modified_at, cmp.cmp_created_at) > to_date('{{ var('load_date') }}', 'yyyy-MM-dd')
)
select
	t.cmp_rowid,
	t.cmp_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	t.cmp_modified_at,
	cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
	t.cmp_deleted_at,
	cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
	-- payload
	t.cmp_channel,
	t.cmp_description,
    -- explicitly address BKs, even when not provided by source (eg. for ghost-record checking)
    cast(t.cmp_rowid as varchar(20)) as cmp_campaign_bk
from rawtable t