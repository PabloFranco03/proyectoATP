{{ config(
    materialized = 'view'
) }}

WITH puntos_saque AS (
    SELECT *
    FROM {{ ref('fct__estadisticas_punto_jugador') }}
    WHERE numero_saque IS NOT NULL
), 

resumen AS (
    SELECT

        COUNT(*) AS total_puntos_saque,

        -- Totales generales
        SUM(ace) AS total_aces,
        SUM(doble_falta) AS total_dobles_faltas,
        SUM(winner) AS total_winners,
        SUM(error_no_forzado) AS total_errores_nf,
        SUM(sube_red) AS total_subidas_red,
        SUM(gana_en_red) AS total_puntos_ganados_en_red,
        SUM(bp_oportunidad) AS total_bp_oportunidades,
        SUM(bp_convertido) AS total_bp_convertidos,
        SUM(bp_fallado) AS total_bp_fallados,

        SUM(CASE WHEN rally_count <= 3 THEN 1 ELSE 0 END) AS total_rally_corto,
        SUM(CASE WHEN rally_count >= 5 THEN 1 ELSE 0 END) AS total_rally_largo,
        SUM(CASE WHEN rally_count = 3 THEN 1 ELSE 0 END) AS total_rally_3,

        SUM(CASE WHEN numero_saque = 1 THEN 1 ELSE 0 END) AS total_saques_1,
        SUM(CASE WHEN numero_saque = 2 THEN 1 ELSE 0 END) AS total_saques_2,

        SUM(CASE WHEN ha_ganado THEN 1 ELSE 0 END) AS total_puntos_ganados,

        -- Medias
        AVG(CASE WHEN numero_saque = 1 AND velocidad_saque > 0 THEN velocidad_saque END) AS vel_media_1er_saque,
        AVG(CASE WHEN numero_saque = 2 AND velocidad_saque > 0 THEN velocidad_saque END) AS vel_media_2do_saque,
        AVG(distancia_recorrida) AS media_distancia_recorrida,
        AVG(rally_count) AS media_rally_count,

        -- Nuevos: dirección lateral
        COUNT_IF(lateral_saque = 'wide') AS total_saque_wide,
        COUNT_IF(lateral_saque = 'body') AS total_saque_body,
        COUNT_IF(lateral_saque = 't') AS total_saque_t,

        -- Nuevos: profundidad
        COUNT_IF(profundidad_saque = 'short') AS total_saque_corto,
        COUNT_IF(profundidad_saque = 'medium') AS total_saque_medio,
        COUNT_IF(profundidad_saque = 'deep') AS total_saque_profundo,

        id_jugador,
        id_torneo_anio

    FROM puntos_saque
    GROUP BY id_jugador, id_torneo_anio
),

-- Enriquecimientos
jugadores AS (
    SELECT id_jugador, nombre_jugador
    FROM {{ ref('dim__jugadores') }}
),

torneo_anio AS (
    SELECT id_torneo_anio, id_torneo, anio_inicio, id_superficie, id_nivel_torneo
    FROM {{ ref('dim__torneo_anio') }}
),

superficie AS (
    SELECT id_superficie, nombre_superficie, es_rapida
    FROM {{ ref('dim__superficie') }}
),

nivel_torneo AS (
    SELECT id_nivel_torneo, nivel_torneo
    FROM {{ ref('dim__nivel_torneo') }}
),

ranking AS (
    SELECT 
        id_jugador, 
        posicion_ranking, 
        puntos, 
        EXTRACT(YEAR FROM ranking_fecha) AS anio_ranking
    FROM {{ ref('dim__ranking_atp') }}
),

enriquecido AS (
    SELECT
        j.nombre_jugador,
        ta.anio_inicio AS anio,
        s.nombre_superficie,
        nt.nivel_torneo,
        rk.posicion_ranking,
        rk.puntos,
        r.*,
        s.es_rapida
    FROM resumen r
    LEFT JOIN jugadores j ON r.id_jugador = j.id_jugador
    LEFT JOIN torneo_anio ta ON r.id_torneo_anio = ta.id_torneo_anio
    LEFT JOIN superficie s ON ta.id_superficie = s.id_superficie
    LEFT JOIN nivel_torneo nt ON ta.id_nivel_torneo = nt.id_nivel_torneo
    LEFT JOIN ranking rk ON r.id_jugador = rk.id_jugador AND ta.anio_inicio = rk.anio_ranking
)

SELECT
    *,

    -- Ratios existentes
    ROUND(CASE WHEN total_saques_1 = 0 THEN NULL ELSE total_aces / total_saques_1 END, 3) AS pct_aces_por_1er_saque,
    ROUND(CASE WHEN total_saques_2 = 0 THEN NULL ELSE total_dobles_faltas / total_saques_2 END, 3) AS pct_df_por_2do_saque,
    ROUND(CASE WHEN total_puntos_saque = 0 THEN NULL ELSE total_winners / total_puntos_saque END, 3) AS pct_winners,
    ROUND(CASE WHEN total_puntos_saque = 0 THEN NULL ELSE total_errores_nf / total_puntos_saque END, 3) AS pct_errores_nf,
    ROUND(CASE WHEN total_puntos_saque = 0 THEN NULL ELSE total_subidas_red / total_puntos_saque END, 3) AS pct_sube_red,
    ROUND(CASE WHEN total_subidas_red = 0 THEN NULL ELSE total_puntos_ganados_en_red / total_subidas_red END, 3) AS pct_efectividad_en_red,
    ROUND(CASE WHEN total_puntos_saque = 0 THEN NULL ELSE total_rally_corto / total_puntos_saque END, 3) AS pct_rally_corto,
    ROUND(CASE WHEN total_puntos_saque = 0 THEN NULL ELSE total_rally_largo / total_puntos_saque END, 3) AS pct_rally_largo,
    ROUND(CASE WHEN total_puntos_saque = 0 THEN NULL ELSE total_puntos_ganados / total_puntos_saque END, 3) AS pct_puntos_ganados_saque,

    -- Porcentajes de dirección lateral
    ROUND(CASE WHEN total_puntos_saque = 0 THEN NULL ELSE total_saque_wide * 1.0 / total_puntos_saque END, 3) AS pct_saque_wide,
    ROUND(CASE WHEN total_puntos_saque = 0 THEN NULL ELSE total_saque_body * 1.0 / total_puntos_saque END, 3) AS pct_saque_body,
    ROUND(CASE WHEN total_puntos_saque = 0 THEN NULL ELSE total_saque_t * 1.0 / total_puntos_saque END, 3) AS pct_saque_t,

    -- Porcentajes de profundidad
    ROUND(CASE WHEN total_puntos_saque = 0 THEN NULL ELSE total_saque_corto * 1.0 / total_puntos_saque END, 3) AS pct_saque_corto,
    ROUND(CASE WHEN total_puntos_saque = 0 THEN NULL ELSE total_saque_medio * 1.0 / total_puntos_saque END, 3) AS pct_saque_medio,
    ROUND(CASE WHEN total_puntos_saque = 0 THEN NULL ELSE total_saque_profundo * 1.0 / total_puntos_saque END, 3) AS pct_saque_profundo

FROM enriquecido
WHERE anio IS NOT NULL
