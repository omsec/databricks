select
	t.*
from {{ ref('vs_user') }} t
where
    current_timestamp between t.load_ts and t.loadend_ts