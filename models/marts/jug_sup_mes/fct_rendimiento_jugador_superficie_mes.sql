{{ config(materialized='table') }}
-- cómo hago la tabla incremental si dependo de posibles campos distintos de ingested_tmz?


WITH partidos AS (
    SELECT
        id_partido,
        id_torneo_anio
    FROM {{ ref('stg_atp_db__partidos') }}
),

estadisticas AS (
    SELECT *
    FROM {{ ref('stg_atp_db__estadisticas_partido_jug') }}
),

torneos_anio AS (
    SELECT
        id_torneo_anio,
        id_superficie,
        anio_inicio AS anio,
        mes_inicio AS mes
    FROM {{ ref('dim__torneo_anio') }}
),

enriquecido AS (
    SELECT
        e.id_jugador,
        t.id_superficie,
        t.anio,
        t.mes,
        e.ha_ganado,
        e.ace,
        e.doble_falta,
        e.puntos_saque,
        e.primeros_saques,
        e.puntos_ganados_1er,
        e.puntos_ganados_2do,
        e.juegos_saque,
        e.bp_salvados,
        e.bp_enfrentados
    FROM estadisticas e
    JOIN partidos p ON e.id_partido = p.id_partido
    JOIN torneos_anio t ON p.id_torneo_anio = t.id_torneo_anio
),

agregado AS (
    SELECT
        id_jugador,
        id_superficie,
        anio,
        mes,

        COUNT(*) AS partidos_jugados,
        SUM(CASE WHEN ha_ganado THEN 1 ELSE 0 END) AS victorias,
        ROUND(SUM(CASE WHEN ha_ganado THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 3) AS ratio_victorias,

        SUM(ace) AS aces_totales,
        ROUND(AVG(ace), 2) AS aces_promedio,

        SUM(doble_falta) AS dobles_faltas_totales,
        ROUND(AVG(doble_falta), 2) AS dobles_faltas_promedio,

        SUM(puntos_saque) AS puntos_saque_totales,
        SUM(primeros_saques) AS primeros_saques_totales,

        ROUND(SUM(primeros_saques) * 1.0 / NULLIF(SUM(puntos_saque), 0), 3) AS ratio_1er_saques_dentro,
        ROUND(SUM(puntos_ganados_1er) * 1.0 / NULLIF(SUM(primeros_saques), 0), 3) AS ratio_1er_saques_ganados,
        ROUND(SUM(puntos_ganados_2do) * 1.0 / NULLIF(SUM(puntos_saque - primeros_saques), 0), 3) AS ratio_2do_saques_ganados,

        SUM(juegos_saque) AS juegos_saque_totales,
        ROUND(AVG(juegos_saque), 2) AS juegos_saque_promedio,

        SUM(bp_enfrentados) AS bp_enfrentados_totales,
        SUM(bp_salvados) AS bp_salvados_totales,
        ROUND(SUM(bp_salvados) * 1.0 / NULLIF(SUM(bp_enfrentados), 0), 3) AS ratio_bp_salvados,

        SUM(bp_enfrentados - bp_salvados) AS breaks_sufridos,
        ROUND(SUM(bp_enfrentados - bp_salvados) * 1.0 / NULLIF(COUNT(*), 0), 3) AS breaks_promedio
    FROM enriquecido
    GROUP BY id_jugador, id_superficie, anio, mes
)

SELECT *
FROM agregado
