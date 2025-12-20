-- By not specifing a unique key, dbt won't update old date; hence facts are "insert only" models

-- ToDo:
-- appearently the above statement is not true. this adds a record for any run :-/

-- https://docs.getdbt.com/docs/build/incremental-strategy

-- incremental_strategy='append',
-- unique_key=['d_date', 'd_product', 'd_customer', 'd_salesman']
-- >>> could not write to file "base/pgsql_tmp/pgsql_tmp6168.69": No space left on device
-- increased mem config, see below

-- FÜR DEN MOMENT 'TABLE'
--> order: vs (änderungen am status erkennen)
--> orderitems (bzw. referenzen) letzter stand; fachlich zu definieren (änderung ja nur im status 'new' erlaubt)

-- In degenrated facts, also include keys of the integrated dims (order)

{{
    config(
        materialized='incremental',
		unique_key=['d_date', 'hk_order', 'd_product', 'd_customer', 'd_salesman']
    )
}}

with src as (
	select
		--to_char(sOrd.ord_sale_ts, 'yyyymmdd')::int as d_date,
		-- this is more acurate than load_ts
        10000 * year(coalesce(sOrd.ord_modified_at, sOrd.ord_created_at)) + 100 * month(coalesce(sOrd.ord_modified_at, sOrd.ord_created_at)) + day(coalesce(sOrd.ord_modified_at, sOrd.ord_created_at)) as d_date,
		bPrd.hk_product_snapshot as d_product,
		pCst.hk_customer_snapshot as d_customer,
		bEmpSm.hk_employee_snapshot as d_salesman,
		bPrd.product_bk,
		pCst.cst_customer_no,
		bEmpSm.emp_employee_no as emp_salesman,
		hOrd.hk_order,
		hOrd.ord_order_no,
		sOrd.ord_sale_ts,
		sOrd.txt_status_en,
		sum(sOrdOit.oit_quantity) as sum_quantity,
		max(sOrdOit.oit_unit_price) as oit_unit_price, -- payload
		sum(sOrdOit.oit_quantity * sOrdOit.oit_unit_price) as sum_unit_price
	from {{ ref('l_order_customer_salesman') }} lOrdCstEmpSM
	-- get the latest state of the combo(s)
	join {{ ref('vls_order_customer_salesman') }} sOrdCstEmpSM
		on  sOrdCstEmpSM.hk_order_customer_salesman = lOrdCstEmpSM.hk_order_customer_salesman
		--and sOrdCstEmpSM.loadend_ts = to_timestamp('2099-12-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss')
	-- hub's used for performance optimization (would work via l_order_customer_salesman)
	join {{ ref('h_order') }} hOrd
		on hOrd.hk_order = lOrdCstEmpSM.hk_order
	join {{ ref('vls_order') }} sOrd
		on sOrd.hk_order = hOrd.hk_order
	join {{ ref('l_order_orderitem') }} lOrdOit
		on lOrdOit.hk_order = hOrd.hk_order
	-- get the latest state of all combos
	join {{ ref('vls_order_orderitem') }} sOrdOit
		on sOrdOit.hk_order_orderitem = lOrdOit.hk_order_orderitem
	join {{ ref('b_product') }} bPrd
		on  bPrd.hk_product = lOrdOit.hk_product
		and sOrd.load_ts between bPrd.valid_from and bPrd.valid_to
	join {{ ref('p_customer') }} pCst
		on  pCst.hk_customer = lOrdCstEmpSM.hk_customer
		and sOrd.load_ts between pCst.valid_from and pCst.valid_to
	join {{ ref('b_employee') }} bEmpSm
		on  bEmpSm.hk_employee = lOrdCstEmpSM.hk_salesman
		and sOrd.load_ts between bEmpSm.valid_from and bEmpSm.valid_to
	--where
		-- status model:
		-- new / {cancelled} / in progress / done
		-- * can't return to new after any modification
		-- * modifications only in status NEW
		-- sOrd.cod_status = '10' -- new
		--and hOrd.ord_order_no = 'ord2018.05/3457.6-75_500_354'
	group by
        10000 * year(coalesce(sOrd.ord_modified_at, sOrd.ord_created_at)) + 100 * month(coalesce(sOrd.ord_modified_at, sOrd.ord_created_at)) + day(coalesce(sOrd.ord_modified_at, sOrd.ord_created_at)),
		bPrd.hk_product_snapshot,
		pCst.hk_customer_snapshot,
		bEmpSm.hk_employee_snapshot,
		bPrd.product_bk,
		pCst.cst_customer_no,
		bEmpSm.emp_employee_no,
		hOrd.hk_order,
		hOrd.ord_order_no,
		sOrd.ord_sale_ts,
		sOrd.txt_status_en
)
select *
from src

{% if is_incremental() %}
	-- only insert **new** records
	where not exists (
		select 1
		from {{ this }} t
		where
			t.d_date = src.d_date
			and t.d_product = src.d_product
			and t.d_customer = src.d_customer
			and t.d_salesman = src.d_salesman
	)
{% endif %}