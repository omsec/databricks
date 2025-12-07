select
	t.*
from {{ ref('_rvs_customer_extended') }} t