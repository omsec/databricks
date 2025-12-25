select
	t.*,
    -- calculated attributes / business rules
    -- this example might a downstream view in real-world :-)
    datediff(year, t.cst_birth_date, current_date()) as cst_age
from {{ ref('pf_customer') }} t