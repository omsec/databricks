-- View containing Business Rules
select
	t.hk_product,
	t.hashdiff,
	t.prd_created_at,
	t.usr_created_by,
	t.prd_modified_at,
	t.usr_modified_by,
	t.prd_deleted_at,
	t.usr_deleted_by,
	t.prd_name,
	t.prd_description,
	-- apply business rules
	coalesce(t.prd_standard_cost, 0) as prd_standard_cost,
	coalesce(t.prd_list_price, 0) as prd_list_price,
	coalesce(t.prd_sold_until, to_date('2099-12-31', 'yyyy-MM-dd')) as prd_sold_until,
	t.load_ts,
	t.record_source,
	t.loadend_ts
from {{ ref('_rvs_product') }} t