select
	t.*
from {{ ref('fh_customer') }} t
where
    current_timestamp between t.valid_from and t.valid_to