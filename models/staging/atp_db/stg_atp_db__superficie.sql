{{ config(materialized='table') }}

WITH raw_matches AS (
    SELECT 
    surface
    FROM {{ source('atp', 'matches') }}
)

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key([limpiar_texto("surface")]) }} AS id_superficie,
    surface AS superficie,

    CASE surface
        WHEN 'Clay' THEN 'Tierra batida'
        WHEN 'Hard' THEN 'Dura'
        WHEN 'Grass' THEN 'Hierba'
        WHEN 'Carpet' THEN 'Moqueta'
        ELSE 'Desconocida'
    END AS nombre_superficie,

    CASE surface
        WHEN 'Hard' THEN TRUE
        WHEN 'Grass' THEN TRUE
        WHEN 'Carpet' THEN TRUE
        WHEN 'Clay' THEN FALSE
        ELSE NULL
    END AS es_rapida

FROM raw_matches
