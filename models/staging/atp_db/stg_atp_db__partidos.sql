{{
    config(
        materialized='incremental',
        unique_key='id_partido'
    )
}}

with source as (
    select *
    from {{ ref('base_atp_db__matches') }}
),

partidos AS (
    SELECT
        match_id_unificada,
        id_partido,
        id_torneo_anio,
        id_ronda_torneo,
        id_ganador,
        id_perdedor,
        duracion_minutos,
        resultado,
        sets_maximos,
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
        numero_partido_torneo,
        ingesta_tmz
    FROM source
    {% if is_incremental() %}
            where ingesta_tmz > (select max(ingesta_tmz) from {{ this }})
    {% endif %} 
),

unificados AS (
    SELECT
        p.*,
        COALESCE(b1.match_id, b2.match_id) AS match_id_unir,
        CASE
            WHEN COALESCE(b1.match_id, b2.match_id) IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS tiene_datos_punto_a_punto,
        CASE
            WHEN match_id_unificada = b1.match_id_unificada_gan1 THEN TRUE
            WHEN match_id_unificada = b2.match_id_unificada_gan2 THEN FALSE
            ELSE NULL
        END AS player1_es_ganador,
        CASE
            WHEN match_id_unificada = b1.match_id_unificada_gan1 THEN id_ganador
            WHEN match_id_unificada = b2.match_id_unificada_gan2 THEN id_perdedor
            ELSE NULL
        END AS id_player1,
        CASE
            WHEN match_id_unificada = b1.match_id_unificada_gan1 THEN id_perdedor
            WHEN match_id_unificada = b2.match_id_unificada_gan2 THEN id_ganador
            ELSE NULL
        END AS id_player2
    FROM partidos p

    LEFT JOIN {{ ref('base_matches_grand_slam') }} b1
        ON match_id_unificada = b1.match_id_unificada_gan1

    LEFT JOIN {{ ref('base_matches_grand_slam') }} b2
        ON match_id_unificada = b2.match_id_unificada_gan2
)

SELECT * FROM unificados
