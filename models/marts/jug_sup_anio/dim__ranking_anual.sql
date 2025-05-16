{{ config(
    materialized = 'table'
) }}

WITH ranking_anual AS (
    SELECT * 
    FROM {{ ref('stg_atp_db__ranking_anual') }}
),

renamed_casted AS (
    SELECT
        id_ranking,
        id_jugador,
        ranking_final,
        puntos_finales,
        anio,
        edad,
        fecha
    FROM ranking_anual
)

SELECT * FROM renamed_casted
