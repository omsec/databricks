select
	t.*
from {{ ref('_rvs_servicerequest_customer') }} t