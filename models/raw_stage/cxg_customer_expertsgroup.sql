with rawtable as (
	select
        cxg.*
	from {{ source('bronze_lz', 'cxg_customer_expertsgroup')}} cxg
    where
        coalesce(cxg.cxg_deleted_at, cxg.cxg_modified_at, cxg.cxg_created_at) > to_date('{{ var('load_date') }}', 'yyyy-MM-dd')
)
select
	t.cxg_rowid,
	t.cxg_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	t.cxg_modified_at,
	cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
	t.cxg_deleted_at,
	cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
	-- payload
	t.exg_group,
	t.cxg_effective_from,
    t.cxg_effective_to,
    t.cxg_level,
    t.cxg_moderator,
    -- explicitly address BKs, even when not provided by source (eg. for ghost-record checking)
    case
        when t.cst_customer is null then '$MISSING'
        else coalesce(cst.cst_customer_no, '$UNKNOWN')
    end as cst_customer_bk,
    case
        when t.exg_group is null then '$MISSING'
        else coalesce(exg.exg_group_bk, '$UNKNOWN')
    end as exg_group_bk
from rawtable t
left outer join {{ ref('cst_customer')}} cst
    on cst.cst_rowid = t.cst_customer
left outer join {{ ref('exg_expertsgroup')}} exg
    on exg.exg_rowid = t.exg_group