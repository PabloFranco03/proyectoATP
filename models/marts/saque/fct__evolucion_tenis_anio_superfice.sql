{{ config(
    materialized = 'view'
) }}

WITH base_filtrada AS (
    SELECT *
    FROM {{ ref('int__rendimiento_jugador_superficie_anio') }}
    WHERE anio BETWEEN 2003 AND 2024
      AND posicion_ranking <= 5
      AND posicion_ranking IS NOT NULL
      AND id_superficie IS NOT NULL
),

agregado AS (
    SELECT
        anio,
        nombre_superficie,
        AVG(altura_cm) AS avg_altura_cm, 

        -- Métricas de servicio
        AVG(aces_por_partido) AS avg_aces_por_partido,
        AVG(dobles_faltas_por_partido) AS avg_dobles_faltas_por_partido,
        AVG(pct_primeros_saques_adentro) AS avg_pct_primeros_saques,
        AVG(pct_puntos_ganados_1er) AS avg_pct_puntos_1er,
        AVG(pct_puntos_ganados_2do) AS avg_pct_puntos_2do,
        AVG(aces_por_juego_saque) AS avg_aces_por_juego,

        id_superficie

    FROM base_filtrada
    GROUP BY anio, id_superficie, nombre_superficie
)

SELECT *
FROM agregado
order by anio
