with ts as (
  select
    t.hk_address,
    t.load_ts,
    coalesce(
      t.adr_deleted_at,
      lead(t.adr_deleted_at) over(partition by t.hk_address order by t.load_ts),
      lead(t.load_ts) over(partition by t.hk_address order by t.load_ts) - interval 1 milliseconds,
      to_timestamp('2099-12-31 23:59:59.999', 'yyyy-MM-dd HH:mm:ss.SSS')
    ) as loadend_ts,
    lead(t.adr_deleted_at) over(partition by t.hk_address order by t.load_ts) as adr_deleted_at,
    lead(t.usr_deleted_by) over(partition by t.hk_address order by t.load_ts) as usr_deleted_by
  from {{ ref('s_address') }} t
)
select *
from ts
where
	ts.load_ts < ts.loadend_ts