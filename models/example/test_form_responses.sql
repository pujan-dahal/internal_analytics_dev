{{ config(materialized='table') }}



with source_table as
(
	select * from {{ source('_airbyte_raw', '_airbyte_raw_form_responses_1') }} 
)
select * from source_table
-- extracted_json as
-- (
-- select
--     jsonb_extract_path_text(_airbyte_data, 'Day') as day,
--     jsonb_extract_path_text(_airbyte_data, 'Timestamp') as timestamp,
--     jsonb_extract_path_text(_airbyte_data, '1. Your Shift') as shift,
--     jsonb_extract_path_text(_airbyte_data, 'Email address') as email,
-- 	jsonb_extract_path_text(_airbyte_data, 'Employee Name') as employee_name,
-- 	jsonb_extract_path_text(_airbyte_data, 'Main Department') as main_department,
-- 	jsonb_extract_path_text(_airbyte_data, 'Sub Department') as sub_department,
-- 	jsonb_extract_path_text(_airbyte_data, '2. Food Coupon ID (write "00" if not taken)') as food_coupon_id,
-- 	jsonb_extract_path_text(_airbyte_data, '3. Will you be using the company drop off service tonight ?') as drop_off_required
-- from source_table
-- ),
-- final_table as
-- (
-- select
-- 	day,
-- 	to_timestamp(timestamp, 'dd/mm/yyyy HH24:MI:SS') as timestamp,
-- 	shift,
-- 	email,
-- 	employee_name,
-- 	main_department,
-- 	sub_department,
-- 	food_coupon_id::integer as food_coupon_id,
-- 	drop_off_required
-- from extracted_json
-- )

-- select * from final_table
