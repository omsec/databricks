with rawtable as (
	select
        cst.*
	from {{ source('bronze_lz', 'cst_customer')}} cst
    where
        coalesce(cst.cst_deleted_at, cst.cst_modified_at, cst.cst_created_at) > to_date('{{ var('load_date') }}', 'yyyy-MM-dd')
),
casted as (
    select
        t.cst_rowid,
        t.cst_created_at,
        cast(t.usr_created_by as varchar(20)) as usr_created_by,
        t.cst_modified_at,
        cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
        t.cst_deleted_at,
        cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
        -- payload
        t.cst_customer_no,
        cast(t.cod_gender as varchar(20)) as cod_gender,
        t.cst_first_name,
        t.cst_last_name,
        t.cst_birth_date,
        cast(t.cod_language as varchar(20)) as cod_language,
        t.cst_culture,
        t.cst_credit_limit,
        t.cmp_campaign,
        t.cst_remark,
        t.cst_attr1,
        t.cst_attr2,
        t.cst_attr3,
        t.cst_attr_int,
        t.cst_attr_str
    from rawtable t
)
select
    cst.*,
    -- always join for these checks
    case
        when cst.cmp_campaign is null then '$MISSING'
        else coalesce(cmp.cmp_campaign_bk, '$UNKNOWN')
    end as cmp_campaign_bk
from casted cst
left outer join {{ ref('cmp_campaign')}} cmp
    on cmp.cmp_rowid = cst.cmp_campaign