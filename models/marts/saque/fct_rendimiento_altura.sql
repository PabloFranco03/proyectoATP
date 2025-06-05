{{ config(
    materialized = 'table'
) }}

WITH base_filtrada AS (
    SELECT *
    FROM {{ ref('int__rendimiento_jugador_superficie_anio') }}
    WHERE anio BETWEEN 2003 AND 2024
      AND altura_cm IS NOT NULL
      AND posicion_ranking <= 100
),

con_buckets AS (
    SELECT *,
        CASE 
            WHEN altura_cm < 180 THEN '<180 cm'
            WHEN altura_cm BETWEEN 180 AND 189 THEN '180–189 cm'
            WHEN altura_cm BETWEEN 190 AND 199 THEN '190–199 cm'
            ELSE '200+ cm'
        END AS altura_bucket
    FROM base_filtrada
),

agregado AS (
    SELECT
        altura_bucket,
        anio,
        COUNT(DISTINCT id_jugador) AS jugadores,
        AVG(aces_por_juego_saque) AS avg_aces_por_juego,
        AVG(pct_aces_sobre_total_puntos) AS avg_pct_aces_total,
        AVG(pct_puntos_ganados_1er) AS avg_pct_puntos_1er,
        AVG(pct_puntos_ganados_2do) AS avg_pct_puntos_2do,
        AVG(pct_break_points_salvados) AS avg_pct_bp_salvados,
        AVG(ratio_aces_dobles_faltas) AS avg_ratio_aces_dobles
    FROM con_buckets
    GROUP BY altura_bucket, anio
)

SELECT *
FROM agregado
ORDER BY anio, altura_bucket
