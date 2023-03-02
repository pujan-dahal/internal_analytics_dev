
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}



with source_table as
(
	select * from {{ source('_airbyte_raw', '_airbyte_raw_books')}}
),
extracted_json as
(
select
    jsonb_extract_path_text(_airbyte_data, 'isbn') as isbn,
    jsonb_extract_path_text(_airbyte_data, 'title') as title,
    jsonb_extract_path_text(_airbyte_data, 'status') as status,
    jsonb_array_elements_text(_airbyte_data -> 'authors') as authors,
    jsonb_extract_path_text(_airbyte_data, 'pageCount')::int as page_count,
    jsonb_array_elements_text(_airbyte_data -> 'categories') as categories,
    jsonb_extract_path_text(_airbyte_data, 'publishedDate')::date as published_date,
    jsonb_extract_path_text(_airbyte_data, 'longDescription') as long_description,
    jsonb_extract_path_text(_airbyte_data, 'shortDescription') as short_description
from source_table
order by authors
),
string_agg_table as
(
select
	isbn,
	title,
	status,
	string_agg(authors, ', ') as authors,
	page_count,
	string_agg(categories, ', ') as categories,
	published_date,
	long_description,
	short_description
from extracted_json
group by 1, 2, 3, 5, 7, 8, 9
),
final_table as 
(
select
	isbn,
	title,
	status,
	replace(trim(replace(authors, 'and', ''), ', '), ', ,', ',') as authors,
	page_count,
	trim(authors, ',') as categories,
	published_date,
	long_description,
	short_description
from string_agg_table
)
select * from final_table

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
