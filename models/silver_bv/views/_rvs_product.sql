select
    t.*,
    coalesce(
        lead(t.load_ts) over (partition by t.hk_product order by t.load_ts) - interval 1 milliseconds,
        t.prd_deleted_at,
        to_timestamp('2099-12-31 23:59:59.999', 'yyyy-MM-dd HH:mm:ss.SSS')
    ) as loadend_ts,
    coalesce(usrc.usr_login_name, t.usr_created_by) as usr_created_by_login_name,
    coalesce(usrm.usr_login_name, t.usr_modified_by) as usr_modified_by_login_name,
    coalesce(usrd.usr_login_name, t.usr_deleted_by) as usr_deleted_by_login_name
from {{ ref('s_product') }} as t
left outer join {{ ref('r_user') }} as usrc
    on t.usr_created_by = usrc.usr_rowid
left outer join {{ ref('r_user') }} as usrm
    on t.usr_modified_by = usrm.usr_rowid
left outer join {{ ref('r_user') }} as usrd
    on t.usr_deleted_by = usrd.usr_rowid
-- Code Look-ups go here
