{{ config(materialized='table') }}

with source_table as
(
	select * from {{ source('_airbyte_raw', '_airbyte_raw_actual_data___drop_off_')}}
),

extracted_json as
(
select
    jsonb_extract_path_text(_airbyte_data, 'Day') as day,
	jsonb_extract_path_text(_airbyte_data, 'Date') as date,
	jsonb_extract_path_text(_airbyte_data, 'Name') as employee_name,
	jsonb_extract_path_text(_airbyte_data, 'Time') as time,
	jsonb_extract_path_text(_airbyte_data, 'Gender') as gender,
	jsonb_extract_path_text(_airbyte_data, 'Status') as status,
	jsonb_extract_path_text(_airbyte_data, 'Address') as address,
	jsonb_extract_path_text(_airbyte_data, 'Department') as department,
	jsonb_extract_path_text(_airbyte_data, 'Shift') as shift,
	jsonb_extract_path_text(_airbyte_data, 'Source') as source
from source_table
),

intermediate_table as
(
select
	upper(trim(day)) as day,
	to_date(date, 'mm/dd/yyyy') as date,
	upper(trim(employee_name)) as employee_name,
	upper(trim(time)) as time,
	upper(trim(gender)) as gender,
	case 
		when upper(trim(status)) is null then 'NOT FILLED'
		else upper(trim(status))
	end as status,
	case 
		when position('(' in address)!=0
			then trim(left(address , position('(' in address)-1))
		else address 
	end as address,
	case
		when address like '%(%)%'
			then trim(substring(address, position('(' in address), position(')' in address)))
		else null
	end as address_remarks,
	trim(source) as source
from extracted_json
),

final_table as
(
select
	day,
	date,
	department,
	md5(employee_name) as employee_name,
	time,
	gender,
	status,
	address,
	address_remarks,
	source
from intermediate_table
)

select * from final_table