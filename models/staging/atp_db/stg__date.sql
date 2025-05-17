{{ config(
    materialized='table'
) }}

WITH date_spine AS (

{{ dbt_utils.date_spine(
    start_date="'2000-01-01'",
    end_date="'2029-12-31'",
    datepart="day"
) }}
)

SELECT 
    date_day AS date,
    EXTRACT(YEAR FROM date_day) AS anio,
    EXTRACT(MONTH FROM date_day) AS mes,
    EXTRACT(DAY FROM date_day) AS dia,
    EXTRACT(DAYOFWEEK FROM date_day) AS dia_semana,
    TRIM(TO_CHAR(date_day, 'MMMM')) AS mes_nombre,
    TRIM(TO_CHAR(date_day, 'DY')) AS dia_nombre
    --CASE 
    --    WHEN date_day <= CURRENT_DATE THEN TRUE
    --    ELSE FALSE
    --END AS es_pasado
FROM date_spine
ORDER BY date_day