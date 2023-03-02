
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}



with source_table as
(
	select * from {{ source_table('_airbyte_raw', '_airbyte_raw_form_responses_1')}} 
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
)

select * from extracted_json

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
