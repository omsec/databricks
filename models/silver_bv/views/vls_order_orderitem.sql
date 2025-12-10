select
	t.*
from {{ ref('vs_order_orderitem') }} t
where
    current_timestamp between t.load_ts and t.loadend_ts