{{ config(
    materialized = 'incremental',
    unique_key = ''
) }}

WITH puntos AS (
    SELECT *
    FROM {{ ref('base_atp_db__points_gran_slam') }}
    WHERE id_partido IS NOT NULL
),

jugador1 AS (
    SELECT
        id_partido,
        id_punto,
        id_player1 AS id_jugador,
        TRUE AS es_player1,
        CASE WHEN punto_winner = 1 THEN TRUE ELSE FALSE END AS ha_ganado_punto,
        CASE WHEN sacador = 1 THEN TRUE ELSE FALSE END AS ha_sacado,
        p1_ace AS ace,
        p1_double_fault AS doble_falta,
        p1_net_point AS sube_red,
        p1_net_point_won AS gana_en_red,
        p1_break_point AS bp_oportunidad,
        p1_break_point_won AS bp_convertido,
        p1_break_point_missed AS bp_fallado,
        ingesta_tmz
    FROM puntos
),

jugador2 AS (
    SELECT
        id_partido,
        id_punto,
        id_player2 AS id_jugador,
        FALSE AS es_player1,
        CASE WHEN punto_winner = 2 THEN TRUE ELSE FALSE END AS ha_ganado_punto,
        CASE WHEN sacador = 2 THEN TRUE ELSE FALSE END AS ha_sacado,
        p2_ace AS ace,
        p2_double_fault AS doble_falta,
        p2_net_point AS sube_red,
        p2_net_point_won AS gana_en_red,
        p2_break_point AS bp_oportunidad,
        p2_break_point_won AS bp_convertido,
        p2_break_point_missed AS bp_fallado,
        ingesta_tmz
    FROM puntos
)

SELECT *
FROM jugador1
UNION ALL
SELECT *
FROM jugador2
{% if is_incremental() %}
  WHERE ingesta_tmz > (SELECT MAX(ingesta_tmz) FROM {{ this }})
{% endif %}
