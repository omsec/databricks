select
	t.*
from {{ ref('vs_expertsgroup_manager') }} t
where
    current_timestamp between t.load_ts and t.loadend_ts