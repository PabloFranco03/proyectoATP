/*{{ config(
    materialized = 'incremental',
    unique_key = 'id_partido'
) }}*/

WITH source AS (
    SELECT *
    FROM {{ source('atp', 'matches') }}
    WHERE {{ filtrado_copa_davis('tourney_level') }}
    /*{% if is_incremental() %}
    WHERE TO_DATE(tourney_date, 'YYYYMMDD') >= (
        SELECT MAX(TO_DATE(tourney_date, 'YYYYMMDD')) FROM {{ this }}
    )
    {% endif %}*/
),

partidos AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['tourney_id', 'match_num']) }} AS id_partido,
        {{ dbt_utils.generate_surrogate_key(['tourney_id']) }} AS id_torneo_anio,
        {{ dbt_utils.generate_surrogate_key(['round']) }} AS id_ronda_torneo,
        {{ dbt_utils.generate_surrogate_key(['winner_id']) }} AS id_ganador,
        {{ dbt_utils.generate_surrogate_key(['loser_id']) }} AS id_perdedor,
        minutes AS duracion_minutos,
        score AS resultado,
        -- Número de sets jugados (coincidencias tipo 6-4, 7-6, etc.)
        --REGEXP_COUNT(score, '\\d+-\\d+') AS sets_jugados,
        -- Sets ganados por el ganador
        --REGEXP_COUNT(score, '(^|\\s)6-|(^|\\s)7-') AS sets_ganador,
        -- Sets ganados por el perdedor: total sets menos sets del ganador
        --(REGEXP_COUNT(score, '\\d+-\\d+') - REGEXP_COUNT(score, '(^|\\s)6-|(^|\\s)7-')) AS sets_perdedor,
        -- Número de tiebreaks jugados (detecta paréntesis tipo 7-6(5))
        --REGEXP_COUNT(score, '\\(\\d+\\)') AS tiebreaks_jugados,
        -- Si el último set fue un tiebreak
        --CASE
            --WHEN REGEXP_SUBSTR(score, '\\(\\d+\\)\\s*$', 1) IS NOT NULL THEN TRUE
            --ELSE FALSE
        --END AS fue_tiebreak_final,
        -- Resultado normalizado: separa sets con barra
        --REPLACE(score, ' ', ' / ') AS resultado_normalizado,
        match_num AS numero_partido_torneo
    FROM source
)

SELECT * FROM partidos
