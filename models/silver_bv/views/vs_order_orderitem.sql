select
	t.*
from {{ ref('_vbs_order_orderitem') }} t