select
	t.*
from {{ ref('_rvs_order') }} t