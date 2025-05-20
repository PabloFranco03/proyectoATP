{{ config(
    materialized='incremental',
    unique_key='id_partido'
) }}

WITH partidos AS (
    SELECT * 
    FROM {{ ref('stg_atp_db__partidos') }}
),
    FROM {{ ref("stg_atp_db__estadisticas_partido_jug")}}

renamed_casted AS (
    SELECT
        id_partido,
        id_torneo_anio,
        id_ronda_torneo,
        id_ganador,
        id_perdedor,
        duracion_minutos,
        resultado,
        sets_maximos,
        numero_partido_torneo,
        ingesta_tmz
        
        -- AÑADIR MACRO

    FROM partidos

    -- INNER JOIN 

    {% if is_incremental() %}
      WHERE s.ingesta_tmz > (SELECT MAX(ingesta_tmz) FROM {{ this }})
    {% endif %}
)

SELECT * FROM renamed_casted
