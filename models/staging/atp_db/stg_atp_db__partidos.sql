{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('atp', 'matches') }}
    WHERE {{ es_torneo_principal('tourney_level') }}
),

partidos AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['tourney_id', 'match_num']) }} AS id_partido,
        tourney_id         AS id_torneo,
        match_num          AS numero_partido,
        round              AS ronda,
        minutes            AS duracion_minutos,
        score              AS resultado,
        winner_id          AS id_ganador,
        loser_id           AS id_perdedor
    FROM source
),

registro_vacio AS (
    SELECT
        NULL AS id_partido,
        NULL AS id_torneo,
        NULL AS numero_partido,
        NULL AS ronda,
        NULL AS duracion_minutos,
        NULL AS resultado,
        NULL AS id_ganador,
        NULL AS id_perdedor
)

SELECT * FROM partidos
UNION ALL
SELECT * FROM registro_vacio
