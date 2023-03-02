{{ config(materialized='table') }}

with source_table as
(
	select * from {{ source('_airbyte_raw', '_airbyte_raw_summary_details_') }} 
),

extracted_json as
(
select
    jsonb_extract_path_text(_airbyte_data, 'Team member name') as employee_name,
    jsonb_extract_path_text(_airbyte_data, 'Department') as department,
    jsonb_extract_path_text(_airbyte_data, 'Gender') as gender,
    jsonb_extract_path_text(_airbyte_data, 'Inside Valley (Y/N)') as inside_valley,
    jsonb_extract_path_text(_airbyte_data, 'Needs Drop off (Y/N) (Applicable for evening shift only)') as needs_drop_off,
    jsonb_extract_path_text(_airbyte_data, 'Drop off location') as drop_off_location,
    jsonb_extract_path_text(_airbyte_data, 'Day 1') as day_1,
    jsonb_extract_path_text(_airbyte_data, 'Day 2') as day_2,
    jsonb_extract_path_text(_airbyte_data, 'Day 3') as day_3,
    jsonb_extract_path_text(_airbyte_data, 'Staff using own vechile (Y/N)') as using_own_vehicle,
    jsonb_extract_path_text(_airbyte_data, 'Location (as per the HR record - Latest information as of Aug 2022)') as address,
    jsonb_extract_path_text(_airbyte_data, 'HODs') as is_hod
from source_table
),

intermediate_table as
(
select
	upper(trim(employee_name)) as employee_name,
	upper(trim(department)) as department,
	upper(trim(gender)) as gender,
	case
		when upper(trim(inside_valley))='YES' then 'Y'
		when upper(trim(inside_valley))='NO' then 'N'
		else 'NOT FILLED'
	end as inside_valley,
	case
		when upper(trim(needs_drop_off))='YES' then 'Y'
		when upper(trim(needs_drop_off))='NO' then 'N'
		else 'NOT FILLED'
	end as needs_drop_off,
	case 
		when drop_off_location = '-' then 'NOT FILLED'
		else drop_off_location
	end as drop_off_location,
	upper(trim(day_1)) as day_1,
	upper(trim(day_2)) as day_2,
	upper(trim(day_3)) as day_3,
	case
      	when upper(trim(using_own_vehicle))='YES' 
      		or upper(trim(using_own_vehicle))='Y' 
      		or upper(trim(using_own_vehicle)) like '%NEED%ALLOWANCE%' then 'Y'
		when upper(trim(using_own_vehicle)) = 'NO'
	    	or upper(trim(using_own_vehicle)) = 'N' then 'N'
	  	else upper(trim(using_own_vehicle))
    end as using_own_vehicle,
    address,
    case 
    	when upper(trim(is_hod))='YES' then 'Y'
    	else null
    end as is_hod
from extracted_json
	
),

final_table as
(
select
	md5(employee_name) as employee_name,
	department,
	gender,
	inside_valley,
	needs_drop_off,
	drop_off_location,
	day_1,
	day_2,
	day_3,
	using_own_vehicle,
	address,
	is_hod
from intermediate_table
)

select * from final_table