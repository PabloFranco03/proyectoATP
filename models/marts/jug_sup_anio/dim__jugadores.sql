{{ config(
    materialized = 'table'
) }}

WITH jugadores AS (
    SELECT * 
    FROM {{ ref('stg_atp_db__jugadores') }}
),

renamed_casted AS (
    SELECT
        id_jugador,
        nombre_jugador,
        mano_dominante,
        CASE 
            WHEN mano_dominante = 'R' THEN 'Diestro'
            WHEN mano_dominante = 'L' THEN 'Zurdo'
            WHEN mano_dominante = 'U' THEN 'Desconocido'
        END AS mano_desc,
        altura_cm,
        cod_pais
    FROM jugadores
)

SELECT * FROM renamed_casted
