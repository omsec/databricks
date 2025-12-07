select
	t.*
from {{ ref('vs_customer_extended') }} t
where
    current_timestamp between t.load_ts and t.loadend_ts