-- Hard Rules go here, eg. Type-Casting
with rawtable as (
	select
	    usr.*
	from {{ source('bronze_lz', 'usr_user')}} usr
    where
        coalesce(usr.usr_deleted_at, usr.usr_modified_at, usr.usr_created_at) > to_date('{{ var('load_date') }}', 'yyyy-MM-dd')
)
select
    -- this avoids run-time casting, eg. in coalesce-statements
	cast(t.usr_rowid as varchar(20)) as usr_rowid,
	t.usr_created_at,
	cast(t.usr_created_by as varchar(20)) as usr_created_by,
	t.usr_modified_at,
	cast(t.usr_modified_by as varchar(20)) as usr_modified_by,
	t.usr_deleted_at,
	cast(t.usr_deleted_by as varchar(20)) as usr_deleted_by,
	t.usr_login_name,
	t.usr_active,
	t.usr_full_name,
	t.usr_last_login
from rawtable t