{{ config(materialized='table') }}

with source_table as
(
	select * from {{ source('_airbyte_raw', '_airbyte_raw_employee_list')}}
),

extracted_json as
(
select
    jsonb_extract_path_text(_airbyte_data, 'NAME') as employee_name,
    jsonb_extract_path_text(_airbyte_data, 'Gender') as gender,
    jsonb_extract_path_text(replace(_airbyte_data::varchar, '\n', '')::jsonb, 'Main Department 2023') as main_department,
    jsonb_extract_path_text(replace(_airbyte_data::varchar, '\n', '')::jsonb, 'Sub-Department 2023') as sub_department,
	jsonb_extract_path_text(_airbyte_data, 'Company Email Address') as email
from source_table
),

intermediate_table as
(
select
	trim(upper(employee_name)) as employee_name,
	trim(upper(gender)) as gender,
	trim(email) as email,
	trim(upper(main_department)) as main_department,
	case
		when sub_department is null then trim(upper(main_department))
		else trim(upper(sub_department))
	end as sub_department
from extracted_json
),

final_table as
(
select
	md5(employee_name) as employee_name,
	gender,
	md5(email) as email,
	main_department,
	sub_department
from intermediate_table
)

select * from final_table


