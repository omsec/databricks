-- Bridges are implemented as incremental dbt models
-- this way, the latest open record (2099) is ended before a new state is inserted
-- Remember, for performance reasons, and with respect of deleted_at these periods
-- are physically saved as a table rather than calculated as a view.
-- (deleted_ts is already applied in the BV-views)

-- Codes are be spared out, since this might be static data and onyl typos are
-- corrected over time. So we might want to apply the latest code descriptions
-- over the entity's entire history.

{{
    config(
        materialized='incremental',
        unique_key=['hk_employee', 'valid_from']
    )
}}

with dates as (
	-- collect all dates when changes happened (detected in data warehouse)
	select distinct
		t.hk_employee,
		t.load_ts as valid_from
	from (
		select
			t.hk_employee,
			t.load_ts
		from {{ ref('h_employee') }} t
		union all
		select
			t.hk_employee,
			t.load_ts
		from {{ ref('s_employee') }} t
	) t
),
periods as (
    -- build periods from these timestamps
    select
        dts.*,
        coalesce(
            lead(dts.valid_from) over(
                partition by
                    dts.hk_employee
                order by
                    dts.valid_from) - interval '1' microsecond,
            to_timestamp('2099-12-31 23:59:59.999999', 'yyyy-MM-dd HH:mm:ss.SSSSSS')) as valid_to
    from dates dts
    --order by dts.hk_customer, dts.valid_from
),
periods_data as (
	select
		dts.hk_employee,
		hEmp.emp_employee_no,
		dts.valid_from,
		dts.valid_to,
		sEmp.load_ts as sEmp_load_ts,
		sEmp.usr_account_id,
		rUsr.usr_login_name,
		sUsr.load_ts as sUsr_load_ts
	from periods dts
	-- this is the "driving hub"
	join {{ ref('h_employee') }} hEmp
		on hEmp.hk_employee = dts.hk_employee
	join {{ ref('vs_employee') }} sEmp
		on  sEmp.hk_employee = dts.hk_employee
		and dts.valid_from between sEmp.load_ts and sEmp.loadend_ts
	-- optional hubs and their satellites must be outer-joined,
	-- since the entity may not have existed at a given point in time,
	-- eg. deleted (remember hub is timeless, ghost-keys will be used)
	left outer join {{ ref('r_user') }} rUsr
		on rUsr.usr_rowid = sEmp.usr_account_id
	left outer join {{ ref('vs_user') }} sUsr
		on  sUsr.usr_rowid = sEmp.usr_account_id
		and dts.valid_from between sUsr.load_ts and sUsr.loadend_ts
),
periods_merged as (
	-- merge consecutive values into one period (compress)
	select
		grp.hk_employee,
		grp.emp_employee_no,
		min(grp.valid_from) as valid_from,
		max(grp.valid_to) as valid_to,
		grp.sEmp_load_ts,
		grp.usr_account_id,
		grp.usr_login_name,
		grp.sUsr_load_ts
	from (
		-- count occurances of values and arrange them in groups
		select
			ts.*,
			-- business key - business key/values
			row_number() over(partition by ts.hk_employee order by ts.valid_from)
				- row_number() over(
					partition by
						ts.hk_employee,
						ts.emp_employee_no,
						ts.sEmp_load_ts,
						ts.usr_account_id,
						ts.usr_login_name,
						ts.sUsr_load_ts
					order by
						ts.valid_from
				) as grp
		from periods_data ts
	) grp
	group by
		grp.hk_employee,
		grp.emp_employee_no,
		grp.sEmp_load_ts,
		grp.usr_account_id,
		grp.sUsr_load_ts,
		grp.usr_login_name,
		grp.grp
)
select
    cast(
        upper(
            sha2(
                concat_ws(
                    '||',
                    ifnull(nullif(trim(cast(t.emp_employee_no as varchar(32))), ''), '^^'),
                    ifnull(nullif(trim(cast(t.valid_from as varchar(32))), ''), '^^')
                ),
            256
            )
        ) as string
    ) as hk_employee_snapshot,
	t.*
from periods_merged t
order by
	t.hk_employee,
	t.valid_from