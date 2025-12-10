select
	t.*
from {{ ref('_rvs_warehouse') }} t