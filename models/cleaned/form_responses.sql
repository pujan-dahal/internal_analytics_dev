{{ config(materialized='table') }}



with source_table as
(
	select * from {{ source('_airbyte_raw', '_airbyte_raw_form_responses_1')}}
),

extracted_json as
(
select
    jsonb_extract_path_text(_airbyte_data, 'Day') as day,
    jsonb_extract_path_text(_airbyte_data, 'Timestamp') as timestamp,
    jsonb_extract_path_text(_airbyte_data, '1. Your Shift') as shift,
    jsonb_extract_path_text(_airbyte_data, 'Email address') as email,
	jsonb_extract_path_text(_airbyte_data, 'Employee Name') as employee_name,
	jsonb_extract_path_text(_airbyte_data, 'Main Department') as main_department,
	jsonb_extract_path_text(_airbyte_data, 'Sub Department') as sub_department,
	jsonb_extract_path_text(_airbyte_data, '2. Food Coupon ID (write "00" if not taken)') as food_coupon_id,
	jsonb_extract_path_text(_airbyte_data, '3. Will you be using the company drop off service tonight ?') as drop_off_required
from source_table
),

intermediate_table as
(
select
	trim(upper(day)) as day,
	to_timestamp(timestamp, 'dd/mm/yyyy HH24:MI:SS')::date as date,
	shift,
	email,
	trim(upper(employee_name)) as employee_name,
	trim(upper(main_department)) as main_department,
	trim(upper(sub_department)) as sub_department,
	case 
		when position('(' in food_coupon_id)!=0
			then trim(left(food_coupon_id, position('(' in food_coupon_id)-1))
		else food_coupon_id
	end as food_coupon_id,
	case
	when food_coupon_id like '%(%)%'
		then trim(substring(food_coupon_id, position('(' in food_coupon_id), position(')' in food_coupon_id)))
	else null
	end as food_coupon_remarks,
	case
		when upper(drop_off_required) like '%YES' then 'YES'
		when upper(drop_off_required) like '%NO'
			or upper(drop_off_required) like '%DAY'
			or upper(drop_off_required) like '%PATHAO' THEN 'NO'
		when upper(drop_off_required) like '%OWN VEHICLE%' then 'OWN VEHICLE'
		when drop_off_required is null then null
		else 'OTHERS'
	end as drop_off_required
from extracted_json
),

final_table as
(
select
	day,
	date,
	shift,
	md5(email) as email,
	md5(employee_name) employee_name,
	main_department,
	sub_department,
	food_coupon_id,
	food_coupon_remarks,
	drop_off_required
from intermediate_table
)

select * from final_table


