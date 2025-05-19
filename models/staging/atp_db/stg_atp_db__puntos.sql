{{ config(
    materialized = 'incremental',
    unique_key = 'id_partido || '-' || numero_punto_partido'
) }}

WITH puntos AS (
    SELECT
        match_id,
        numero_juego AS id_juego,
        numero_punto_partido AS id_punto,
        ganador_punto AS punto_winner,
        rally,
        p1_score,
        p2_score,
        ingesta_tmz
    FROM {{ ref('base_extra_grand_slam__puntos_gran_slam') }}
    WHERE numero_punto_partido IS NOT NULL
),

mapping AS (
    SELECT
        id_partido_otro,
        id_partido,
        id_player1,
        id_player2
    FROM {{ ref('int_match_id_mapping') }}
)

SELECT
    m.id_partido,
    p.id_juego,
    p.id_punto,
    p.punto_winner,
    CASE 
        WHEN p.punto_winner = 1 THEN m.id_player2
        WHEN p.punto_winner = 2 THEN m.id_player1
        ELSE NULL
    END AS punto_loser,
    p.rally,
    CONCAT(p.p1_score, '-', p.p2_score) AS resultado_momento_saque,
    p.ingesta_tmz
FROM puntos p
LEFT JOIN mapping m
  ON p.match_id = m.id_partido_otro
