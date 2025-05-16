{{ config(
    materialized = 'table'
) }}

WITH stg_fecha AS (
    SELECT * 
    FROM {{ ref('stg__date') }}
),

dim_fecha AS (

    SELECT 
        date AS fecha,
        anio,
        mes,
        dia,
        dia_semana,
        mes_nombre,
        dia_nombre,
        es_pasado
    FROM stg_fecha

)

SELECT * FROM dim_fecha
