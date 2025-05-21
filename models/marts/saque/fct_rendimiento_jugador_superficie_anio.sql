{{ config(
    materialized='incremental',
    unique_key='',
) }}

-- añadir if incremental

WITH partidos_jugador AS (
    SELECT *
    FROM {{ ref('fct__estadisticas_jugador_partido') }}
),

torneo_anio AS (
    SELECT id_torneo_anio, id_superficie, anio_inicio AS anio
    FROM {{ ref('dim__torneo_anio') }}
    WHERE anio_inicio IS NOT NULL
),

partidos_enriquecidos AS (
    SELECT 
        p.id_jugador,
        t.anio,
        t.id_superficie,
        p.ha_ganado,
        p.aces,
        p.dobles_faltas,
        p.puntos_saque,
        p.primeros_saques,
        p.puntos_ganados_1er,
        p.puntos_ganados_2do,
        p.juegos_saque,
        p.bp_salvados,
        p.bp_enfrentados
    FROM partidos_jugador p
    LEFT JOIN torneo_anio t 
        ON p.id_torneo_anio = t.id_torneo_anio
),

resumen AS (
    SELECT
        id_jugador,
        id_superficie,
        anio,
        COUNT(*) AS partidos_disputados,
        SUM(CASE WHEN ha_ganado THEN 1 ELSE 0 END) AS partidos_ganados,
        SUM(aces) AS total_aces,
        SUM(dobles_faltas) AS total_dobles_faltas,
        SUM(puntos_saque) AS total_puntos_saque,
        SUM(primeros_saques) AS total_primeros_saques,
        SUM(puntos_ganados_1er) AS total_puntos_ganados_1er,
        SUM(puntos_ganados_2do) AS total_puntos_ganados_2do,
        SUM(juegos_saque) AS total_juegos_saque,
        SUM(bp_salvados) AS total_bp_salvados,
        SUM(bp_enfrentados) AS total_bp_enfrentados
    FROM partidos_enriquecidos
    GROUP BY id_jugador, id_superficie, anio
),

ranking_final_anio AS (
    SELECT
        id_jugador,
        EXTRACT(YEAR FROM ranking_fecha) AS anio,
        posicion_ranking
    FROM {{ ref('dim__ranking_atp') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_jugador, EXTRACT(YEAR FROM ranking_fecha) ORDER BY ranking_fecha DESC) = 1
),

jugadores AS (
    SELECT id_jugador, altura_cm
    FROM {{ ref('dim__jugadores') }}
)

SELECT
    r.*,
    j.altura_cm,
    rk.posicion_ranking,

    -- Métricas porcentuales y derivadas
    SAFE_DIVIDE(total_primeros_saques, total_puntos_saque) AS pct_primeros_saques_adentro,
    SAFE_DIVIDE(total_puntos_saque - total_primeros_saques, total_puntos_saque) AS pct_segundos_saques_jugados,
    SAFE_DIVIDE(total_puntos_ganados_1er, total_primeros_saques) AS pct_puntos_ganados_1er,
    SAFE_DIVIDE(total_puntos_ganados_2do, total_puntos_saque - total_primeros_saques) AS pct_puntos_ganados_2do,
    SAFE_DIVIDE(partidos_ganados, partidos_disputados) AS pct_partidos_ganados,
    SAFE_DIVIDE(total_bp_salvados, total_bp_enfrentados) AS pct_break_points_salvados,
    SAFE_DIVIDE(total_juegos_saque, partidos_disputados) AS juegos_saque_por_partido,
    SAFE_DIVIDE(total_aces, partidos_disputados) AS aces_por_partido,
    SAFE_DIVIDE(total_aces, total_juegos_saque) AS aces_por_juego_saque,
    SAFE_DIVIDE(total_aces, total_primeros_saques) AS pct_aces_por_primer_saque,
    SAFE_DIVIDE(total_aces, total_puntos_saque) AS pct_aces_sobre_total_puntos,
    SAFE_DIVIDE(total_dobles_faltas, partidos_disputados) AS dobles_faltas_por_partido,
    SAFE_DIVIDE(total_dobles_faltas, total_puntos_saque - total_primeros_saques) AS pct_dobles_faltas_sobre_2dos_saques,
    SAFE_DIVIDE(total_aces, NULLIF(total_dobles_faltas, 0)) AS ratio_aces_dobles_faltas

FROM resumen r
LEFT JOIN jugadores j ON r.id_jugador = j.id_jugador
LEFT JOIN ranking_final_anio rk ON r.id_jugador = rk.id_jugador AND r.anio = rk.anio
