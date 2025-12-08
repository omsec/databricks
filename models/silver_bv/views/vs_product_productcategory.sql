select
	t.*,
    -- link sammelt alle kombinationen, hier werden sie interpretiert gemäss Fachregeln:
    -- jede kombination nur 1x
    -- neue kombination = neue gültigkeit
    -- somit kann über den driving key (der stabile, referenzierende) gruppiert werden
    coalesce(lead(t.load_ts) over(partition by t.hk_product order by t.load_ts) - interval 1 milliseconds, to_timestamp('2099-12-31 23:59:59.999', 'yyyy-mm-dd hh24:mi:ss.fff')) as loadend_ts
from {{ ref('s_product_productcategory') }} t