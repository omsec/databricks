select
	t.*
from {{ ref('_rvs_order_customer_salesman') }} t