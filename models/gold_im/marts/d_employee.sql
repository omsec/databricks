
-- SCD1: Only apply the latest version (overwrite changes)
-- https://ijaniszewski.medium.com/data-engineering-understanding-slowly-changing-dimensions-scd-with-dbt-98b37f3ddf03

-- Since it's so easy, all dimensions are SCD2 (materialized=incremental) - except for the d_date

{{
    config(
        materialized='incremental',
        unique_key='d_employee'
    )
}}

select
	bEmp.hk_employee_snapshot as d_employee,
	bEmp.emp_employee_no,
	bEmp.valid_from,
	bEmp.valid_to,
	sEmp.emp_first_name,
	sEmp.emp_last_name,
	sEmp.emp_email,
	sEmp.emp_phone,
	sEmp.emp_job_title,
	bEmp.usr_account_id,
	bEmp.usr_login_name
from{{ ref('b_employee') }} bEmp
join{{ ref('vs_employee') }} sEmp
	on  sEmp.hk_employee = bEmp.hk_employee
	and sEmp.load_ts = bEmp.semp_load_ts
order by
	bEmp.valid_from,
	bEmp.emp_employee_no