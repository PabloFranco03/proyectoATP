{{ config(
    materialized='incremental',
    unique_key='id_partido'
) }}

WITH source AS (
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
        -- Número de sets jugados (coincidencias tipo 6-4, 7-6, etc.)
        --REGEXP_COUNT(score, '\d+-\d+') AS sets_jugados,
        -- Sets ganados por el ganador
        --REGEXP_COUNT(score, '(^|\s)6-|(^|\s)7-') AS sets_ganador,
        -- Sets ganados por el perdedor: total sets menos sets del ganador
        --(REGEXP_COUNT(score, '\d+-\d+') - REGEXP_COUNT(score, '(^|\s)6-|(^|\s)7-')) AS sets_perdedor,
        -- Número de tiebreaks jugados (detecta paréntesis tipo 7-6(5))
        --REGEXP_COUNT(score, '\(\d+\)') AS tiebreaks_jugados,
        -- Si el último set fue un tiebreak
        --CASE
            --WHEN REGEXP_SUBSTR(score, '\(\d+\)\s*$', 1) IS NOT NULL THEN TRUE
            --ELSE FALSE
        --END AS fue_tiebreak_final,
        -- Resultado normalizado: separa sets con barra
        --REPLACE(score, ' ', ' / ') AS resultado_normalizado,
    FROM {{ ref('base_atp_db__matches') }}
),

mapping AS (
    SELECT *
    FROM {{ ref('int__union_ids') }}
),

partidos AS (
    SELECT
        s.*,
        m.ganador,
        m.id_player1,
        m.id_player2
    FROM source s
    LEFT JOIN mapping m
      ON s.id_partido = m.id_partido
    {% if is_incremental() %}
      WHERE s.ingesta_tmz > (SELECT MAX(ingesta_tmz) FROM {{ this }})
    {% endif %}
)

SELECT * FROM partidos