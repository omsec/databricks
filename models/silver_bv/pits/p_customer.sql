-- PITs are implemented as incremental dbt models
-- this way, the latest open record (2099) is ended before a new state is inserted.
-- Remember, for performance reasons, these periods are physically saved as a table
-- rather than calculated as a view.
-- Deleted records are included, as they may be needed for patching/business views.

-- Codes are be spared out, since this might be static data and onyl typos are
-- corrected over time. So we might want to apply the latest code descriptions
-- over the entity's entire history.

-- Users are not part of pits, as they are joined in higher views
-- (usually the latest version)

-- PITs directly access RV-tables; as periods are built within from collected timestamps

{{
    config(
        materialized='incremental',
        unique_key=['hk_customer', 'valid_from']
    )
}}

with dates as (
	-- collect all timestamps when changes happened (detected in data warehouse)
	select
		hCst.hk_customer,
		hCst.load_ts as hCst_load_ts,
		sCst.load_ts as sCst_load_ts,
		sCstExt.load_ts as sCstExt_load_ts,
		sCstM.load_ts as sCstM_load_ts
	from silver_rv.h_customer hCst
	-- use outer joins where necessary
	join {{ ref('s_customer') }} sCst
		on sCst.hk_customer = hCst.hk_customer
	join {{ ref('s_customer_extended') }} sCstExt
		on sCstExt.hk_customer = hCst.hk_customer
	join {{ ref('s_customer_meta') }} sCstM
		on sCstM.hk_customer = hCst.hk_customer
),
dates_unpivot (
	select distinct
	  dt.hk_customer,
	  valid_from
	from dates dt unpivot
	(valid_from for load_ts in (hCst_load_ts, sCst_load_ts, sCstExt_load_ts, sCstM_load_ts))
),
periods as (
	-- build periods from these timestamps
	select
		dts.*,
		coalesce(
			lead(dts.valid_from) over(
				partition by
					dts.hk_customer
				order by
					dts.valid_from) - interval '1' microsecond,
			to_timestamp('2099-12-31 23:59:59.999999', 'yyyy-MM-dd HH:mm:ss.SSSSSS')) as valid_to
	from dates_unpivot dts
	--order by dts.hk_customer, dts.valid_from
),
periods_data as (
    -- get the latest load_ts for each period (last of day)
    select
        dts.hk_customer,
        hCst.cst_customer_no, -- denorm for usability
        dts.valid_from,
        dts.valid_to,
        --max(hCst.load_ts) as hCst_load_ts, -- not needed, it stays the same
        max(sCst.load_ts) as sCst_load_ts,
        max(sCstExt.load_ts) as sCstExt_load_ts,
        max(sCstM.load_ts) as sCstM_load_ts
    from periods dts
	-- using outer joins, since we deal with different history-lines
    left outer join {{ ref('h_customer') }} hCst
        on hCst.hk_customer = dts.hk_customer
    left outer join {{ ref('s_customer') }} sCst
        on  sCst.hk_customer = dts.hk_customer
        and dts.valid_from = sCst.load_ts
    left outer join {{ ref('s_customer_extended') }} sCstExt
        on  sCstExt.hk_customer = dts.hk_customer
        and dts.valid_from = sCstExt.load_ts
    left outer join {{ ref('s_customer_meta') }} sCstM
        on  sCstM.hk_customer = dts.hk_customer
        and dts.valid_from = sCstM.load_ts
    group by
        dts.hk_customer,
        hCst.cst_customer_no, -- denorm for usability
        dts.valid_from,
        dts.valid_to
),
periods_merged as (
    -- merge consecutive values into one period (compress)
	select
		grp.hk_customer,
		grp.cst_customer_no,
		min(grp.valid_from) as valid_from,
		max(grp.valid_to) as valid_to,
		--grp.hCst_load_ts,
		grp.sCst_load_ts,
		grp.sCstExt_load_ts,
		grp.sCstM_load_ts
	from (
		-- count occurances of values and arrange them in groups
		select
			ts.*,
			-- business key - business key/values
			row_number() over(partition by ts.hk_customer order by ts.valid_from)
				- row_number() over(
					partition by
						ts.hk_customer,
						--ts.hCst_load_ts,
						ts.sCst_load_ts,
						ts.sCstExt_load_ts,
						ts.sCstM_load_ts
					order by
						ts.valid_from) as grp
		from periods_data ts
		--order by valid_from
	) grp
	group by
		grp.hk_customer,
		grp.cst_customer_no,
		--grp.hCst_load_ts,
		grp.sCst_load_ts,
		grp.sCstExt_load_ts,
		grp.sCstM_load_ts,
		grp.grp
	--order by valid_from
)
select
    cast(
        upper(
            sha2(
                concat_ws(
                    '||',
                    ifnull(nullif(trim(cast(t.cst_customer_no as varchar(32))), ''), '^^'),
                    ifnull(nullif(trim(cast(t.valid_from as varchar(32))), ''), '^^')
                ),
            256
            )
        ) as string
    ) as hk_customer_snapshot,
    t.*
from periods_merged t