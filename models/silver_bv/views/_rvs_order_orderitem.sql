select
	t.*,
	-- 1:0..1 link
	-- every combination just once
	-- new combination means new period/validity
	-- hence the driving key (stable reference) is used for grouping / building periods
    coalesce(
		lead(t.load_ts) over(partition by t.hk_order_orderitem order by t.load_ts) - interval 1 milliseconds, to_timestamp('2099-12-31 23:59:59.999', 'yyyy-MM-dd HH:mm:ss.SSS')) as loadend_ts
from {{ ref('s_order_orderitem') }} t
-- User Look-ups go here
-- Code Look-ups go here