with rawtable as (
	select
        csb.*
	from {{ source('bronze_lz', 'csb_customer_badge')}} csb
    where
        coalesce(csb.csb_deleted_at, csb.csb_modified_at, csb.csb_created_at) > to_date('{{ var('load_date') }}', 'yyyy-MM-dd')
)
select
	t.csb_rowid,
	t.csb_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	t.csb_modified_at,
	cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
	t.csb_deleted_at,
	cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
	-- payload
	t.cst_customer,
    t.bdg_badge,
    -- explicitly address BKs, even when not provided by source (eg. for ghost-record checking)
    case
        when t.cst_customer is null then '$MISSING'
        else coalesce(cst.cst_customer_no, '$UNKNOWN')
    end as cst_customer_bk,
    case
        when t.bdg_badge is null then '$MISSING'
        else coalesce(bdg.bdg_badge_bk, '$UNKNOWN')
    end as bdg_badge_bk
from rawtable t
left outer join {{ ref('cst_customer')}} cst
    on cst.cst_rowid = t.cst_customer
left outer join {{ ref('bdg_badge')}} bdg
    on bdg.bdg_rowid = t.bdg_badge