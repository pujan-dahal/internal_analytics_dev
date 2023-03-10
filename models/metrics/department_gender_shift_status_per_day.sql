----------PER DAY DEPARTMENT AND GENDER WISE SHIFT STATS----------
{{ config(materialized='view') }}

SELECT
    date,
    department,
    gender,
    SUM(CASE WHEN LOWER(shift)= 'day' THEN 1 ELSE 0 END) AS day_shift,
    SUM(CASE WHEN LOWER(shift)= 'evening' THEN 1 ELSE 0 END) AS evening_shift,
FROM {{ ref('actual_data_drop_off') }} 
GROUP BY Date, department, gender
ORDER BY Date