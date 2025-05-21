{{ config(
    materialized='table'
) }}

WITH rendimiento_base AS (
    SELECT *
    FROM {{ ref('fct_rendimiento_jugador_superficie_anio') }}
    WHERE posicion_ranking IS NOT NULL
),

top10 AS (
    SELECT *
    FROM rendimiento_base
    WHERE posicion_ranking <= 10
),

resumen_surface_anio AS (
    SELECT
        id_superficie,
        anio,

        COUNT(*) AS jugadores_top10,
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
        SUM(total_bp_enfrentados) AS total_bp_enfrentados,
        AVG(altura_cm) AS altura_media_cm,

        SAFE_DIVIDE(SUM(total_primeros_saques), SUM(total_puntos_saque)) AS pct_primeros_saques_adentro,
        SAFE_DIVIDE(SUM(total_puntos_saque) - SUM(total_primeros_saques), SUM(total_puntos_saque)) AS pct_segundos_saques_jugados,
        SAFE_DIVIDE(SUM(total_puntos_ganados_1er), SUM(total_primeros_saques)) AS pct_puntos_ganados_1er,
        SAFE_DIVIDE(SUM(total_puntos_ganados_2do), SUM(total_puntos_saque) - SUM(total_primeros_saques)) AS pct_puntos_ganados_2do,
        SAFE_DIVIDE(SUM(partidos_ganados), SUM(partidos_disputados)) AS pct_partidos_ganados,
        SAFE_DIVIDE(SUM(total_bp_salvados), SUM(total_bp_enfrentados)) AS pct_break_points_salvados,
        SAFE_DIVIDE(SUM(total_juegos_saque), SUM(partidos_disputados)) AS juegos_saque_por_partido,
        SAFE_DIVIDE(SUM(total_aces), SUM(partidos_disputados)) AS aces_por_partido,
        SAFE_DIVIDE(SUM(total_aces), SUM(total_juegos_saque)) AS aces_por_juego_saque,
        SAFE_DIVIDE(SUM(total_aces), SUM(total_primeros_saques)) AS pct_aces_por_primer_saque,
        SAFE_DIVIDE(SUM(total_aces), SUM(total_puntos_saque)) AS pct_aces_sobre_total_puntos,
        SAFE_DIVIDE(SUM(total_dobles_faltas), SUM(partidos_disputados)) AS dobles_faltas_por_partido,
        SAFE_DIVIDE(SUM(total_dobles_faltas), SUM(total_puntos_saque) - SUM(total_primeros_saques)) AS pct_dobles_faltas_sobre_2dos_saques,
        SAFE_DIVIDE(SUM(total_aces), NULLIF(SUM(total_dobles_faltas), 0)) AS ratio_aces_dobles_faltas

    FROM top10
    GROUP BY id_superficie, anio
),

superficie AS (
    SELECT id_superficie, nombre_superficie
    FROM {{ ref('dim__superficie') }}
)

SELECT 
    s.nombre_superficie,
    r.anio,
    r.jugadores_top10,
    r.altura_media_cm,

    r.pct_primeros_saques_adentro,
    r.pct_segundos_saques_jugados,
    r.pct_puntos_ganados_1er,
    r.pct_puntos_ganados_2do,
    r.pct_partidos_ganados,
    r.pct_break_points_salvados,
    r.juegos_saque_por_partido,
    r.aces_por_partido,
    r.aces_por_juego_saque,
    r.pct_aces_por_primer_saque,
    r.pct_aces_sobre_total_puntos,
    r.dobles_faltas_por_partido,
    r.pct_dobles_faltas_sobre_2dos_saques,
    r.ratio_aces_dobles_faltas,

    r.partidos_disputados,
    r.partidos_ganados,
    r.total_aces,
    r.total_dobles_faltas,
    r.total_puntos_saque,
    r.total_primeros_saques,
    r.total_puntos_ganados_1er,
    r.total_puntos_ganados_2do,
    r.total_juegos_saque,
    r.total_bp_salvados,
    r.total_bp_enfrentados
    
FROM resumen_surface_anio r
LEFT JOIN superficie s ON r.id_superficie = s.id_superficie
