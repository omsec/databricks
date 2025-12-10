select
	t.*
from {{ ref('_rvs_order_address') }} t