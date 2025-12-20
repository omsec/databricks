select
	t.*,
    -- calcalated attributes / business rules
    -- this example might a view in real-world :-)
    datediff(year, t.cst_birth_date, current_date()) as cst_age
from {{ ref('_rvs_customer') }} t