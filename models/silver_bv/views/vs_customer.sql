select
	t.*
from {{ ref('_vbs_customer') }} t