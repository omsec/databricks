select
	t.*
from {{ ref('_rvs_customer') }} t