/*{{ config(
    materialized = 'incremental',
    unique_key = 'id_partido'
) }}*/

WITH source AS (
    SELECT *
    FROM {{ source('atp', 'matches') }}
    WHERE {{ es_torneo_principal('tourney_level') }}
    /*{% if is_incremental() %}
    AND TO_DATE(tourney_date, 'YYYYMMDD') >= (
        SELECT MAX(TO_DATE(tourney_date, 'YYYYMMDD')) FROM {{ this }}
    )
    {% endif %}*/
),

partidos AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['tourney_id', 'match_num']) }} AS id_partido,
        {{ dbt_utils.generate_surrogate_key(['tourney_id']) }} AS id_torneo_anio,
        match_num AS numero_partido_torneo,
        round AS ronda,
        minutes AS duracion_minutos,
        score AS resultado,
        {{ dbt_utils.generate_surrogate_key(['winner_id']) }} AS id_ganador,
        {{ dbt_utils.generate_surrogate_key(['loser_id']) }} AS id_perdedor
    FROM source
),

registro_vacio AS (
    SELECT
        NULL AS id_partido,
        NULL AS id_torneo_anio,
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
