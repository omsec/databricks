-- View containing Business Rules
select
	t.hk_order_orderitem,
	t.rowhash,
    -- HKs (denorm)
    t.oit_rowid,
	t.oit_created_at,
	t.usr_created_by,
	t.oit_modified_at,
	t.usr_modified_by,
	t.oit_deleted_at,
	t.usr_deleted_by,
    -- payload & business rules
    t.oit_quantity,
    coalesce(t.oit_unit_price, 0) as oit_unit_price,
    t.oit_ord_effective_from,
	t.oit_attr_int,
	t.oit_attr_str,
    t.load_ts,
    t.loadend_ts,
	t.record_source
from {{ ref('_rvs_order_orderitem') }} t