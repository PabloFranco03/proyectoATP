{{ config(
    materialized = 'table'
) }}

WITH base AS (
    SELECT *
    FROM {{ ref('int__rendimiento_jugador_superficie_anio') }}
    WHERE posicion_ranking <= 10
),

agrupado AS (
    SELECT
        nombre_superficie,
        anio,
        AVG(altura_cm) AS altura_media_cm,

        CASE WHEN SUM(total_puntos_saque) = 0 THEN NULL ELSE SUM(total_primeros_saques) / SUM(total_puntos_saque) END AS pct_primeros_saques_adentro,
        CASE WHEN SUM(total_puntos_saque) = 0 THEN NULL ELSE (SUM(total_puntos_saque) - SUM(total_primeros_saques)) / SUM(total_puntos_saque) END AS pct_segundos_saques_jugados,
        CASE WHEN SUM(total_primeros_saques) = 0 THEN NULL ELSE SUM(total_puntos_ganados_1er) / SUM(total_primeros_saques) END AS pct_puntos_ganados_1er,
        CASE WHEN (SUM(total_puntos_saque) - SUM(total_primeros_saques)) = 0 THEN NULL ELSE SUM(total_puntos_ganados_2do) / (SUM(total_puntos_saque) - SUM(total_primeros_saques)) END AS pct_puntos_ganados_2do,
        CASE WHEN SUM(partidos_disputados) = 0 THEN NULL ELSE SUM(partidos_ganados) / SUM(partidos_disputados) END AS pct_partidos_ganados,
        CASE WHEN SUM(total_bp_enfrentados) = 0 THEN NULL ELSE SUM(total_bp_salvados) / SUM(total_bp_enfrentados) END AS pct_break_points_salvados,
        CASE WHEN SUM(partidos_disputados) = 0 THEN NULL ELSE SUM(total_juegos_saque) / SUM(partidos_disputados) END AS juegos_saque_por_partido,
        CASE WHEN SUM(partidos_disputados) = 0 THEN NULL ELSE SUM(total_aces) / SUM(partidos_disputados) END AS aces_por_partido,
        CASE WHEN SUM(total_juegos_saque) = 0 THEN NULL ELSE SUM(total_aces) / SUM(total_juegos_saque) END AS aces_por_juego_saque,
        CASE WHEN SUM(total_primeros_saques) = 0 THEN NULL ELSE SUM(total_aces) / SUM(total_primeros_saques) END AS pct_aces_por_primer_saque,
        CASE WHEN SUM(total_puntos_saque) = 0 THEN NULL ELSE SUM(total_aces) / SUM(total_puntos_saque) END AS pct_aces_sobre_total_puntos,
        CASE WHEN SUM(partidos_disputados) = 0 THEN NULL ELSE SUM(total_dobles_faltas) / SUM(partidos_disputados) END AS dobles_faltas_por_partido,
        CASE WHEN (SUM(total_puntos_saque) - SUM(total_primeros_saques)) = 0 THEN NULL ELSE SUM(total_dobles_faltas) / (SUM(total_puntos_saque) - SUM(total_primeros_saques)) END AS pct_dobles_faltas_sobre_2dos_saques,
        CASE WHEN SUM(total_dobles_faltas) = 0 THEN NULL ELSE SUM(total_aces) / SUM(total_dobles_faltas) END AS ratio_aces_dobles_faltas,


        -- Totales
        SUM(partidos_disputados) AS partidos_disputados,
        SUM(partidos_ganados) AS partidos_ganados,
        SUM(total_aces) AS total_aces,
        SUM(total_dobles_faltas) AS total_dobles_faltas,
        SUM(total_puntos_saque) AS total_puntos_saque,
        SUM(total_primeros_saques) AS total_primeros_saques,
        SUM(total_puntos_ganados_1er) AS total_puntos_ganados_1er,
        SUM(total_puntos_ganados_2do) AS total_puntos_ganados_2do,
        SUM(total_juegos_saque) AS total_juegos_saque,
        SUM(total_bp_salvados) AS total_bp_salvados,
        SUM(total_bp_enfrentados) AS total_bp_enfrentados

    FROM base
    GROUP BY id_superficie, nombre_superficie, anio
)

SELECT * FROM agrupado
order by anio asc
