{{ config(
    materialized = 'table'
) }}

WITH base AS (
    SELECT *
    FROM {{ ref('int__rendimiento_saque_jug_torneo') }}
    WHERE anio BETWEEN 2003 AND 2024
      AND posicion_ranking <= 5
)

SELECT
    anio,
    nombre_superficie,
    
    -- Velocidades promedio
    AVG(vel_media_1er_saque) AS avg_velocidad_1er,
    AVG(vel_media_2do_saque) AS avg_velocidad_2do,

    -- Efectividad del saque
    AVG(pct_puntos_ganados_saque) AS avg_pct_puntos_ganados_saque,

    -- Estilo de juego asociado al saque
    AVG(pct_rally_corto) AS avg_pct_rally_corto,
    AVG(media_distancia_recorrida) AS avg_distancia_recorrida

FROM base
GROUP BY anio, nombre_superficie
ORDER BY anio, nombre_superficie
