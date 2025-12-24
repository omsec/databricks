select
	t.*,
    coalesce(lead(t.load_ts) over(partition by t.hk_customer order by t.load_ts) - interval 1 milliseconds, to_timestamp('2099-12-31 23:59:59.999', 'yyyy-MM-dd HH:mm:ss.SSS')) as loadend_ts
from {{ ref('s_customer_meta') }} t