{{ config(
    materialized = 'table'
) }}

WITH puntos_saque AS (
    SELECT *
    FROM {{ ref('fct__estadisticas_punto_jugador') }}
    WHERE numero_saque IS NOT NULL
),

resumen AS (
    SELECT
        id_jugador,
        id_torneo_anio,

        COUNT(*) AS total_puntos_saque,

        -- Totales
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
        SUM(CASE WHEN rally_count = 3 AND ha_ganado THEN 1 ELSE 0 END) AS rally_3_ganados,

        SUM(CASE WHEN numero_saque = 1 THEN 1 ELSE 0 END) AS total_saques_1,
        SUM(CASE WHEN numero_saque = 2 THEN 1 ELSE 0 END) AS total_saques_2,

        SUM(CASE WHEN ha_ganado THEN 1 ELSE 0 END) AS total_puntos_ganados,
        
        -- Medias
        AVG(CASE WHEN numero_saque = 1 AND velocidad_saque > 0 THEN velocidad_saque END) AS vel_media_1er_saque,
        AVG(CASE WHEN numero_saque = 2 AND velocidad_saque > 0 THEN velocidad_saque END) AS vel_media_2do_saque,
        AVG(distancia_recorrida) AS media_distancia_recorrida,
        AVG(rally_count) AS media_rally_count

    FROM puntos_saque
    GROUP BY id_jugador, id_torneo_anio
),

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

enriquecido AS (
    SELECT
        r.*,
        j.nombre_jugador,
        ta.anio_inicio AS anio,
        s.nombre_superficie,
        s.es_rapida,
        nt.nivel_torneo
    FROM resumen r
    LEFT JOIN jugadores j ON r.id_jugador = j.id_jugador
    LEFT JOIN torneo_anio ta ON r.id_torneo_anio = ta.id_torneo_anio
    LEFT JOIN superficie s ON ta.id_superficie = s.id_superficie
    LEFT JOIN nivel_torneo nt ON ta.id_nivel_torneo = nt.id_nivel_torneo
)

SELECT
    *,
    -- Porcentajes y ratios
    SAFE_DIVIDE(total_aces, total_saques_1) AS pct_aces_por_1er_saque,
    SAFE_DIVIDE(total_dobles_faltas, total_saques_2) AS pct_df_por_2do_saque,
    SAFE_DIVIDE(total_winners, total_puntos_saque) AS pct_winners,
    SAFE_DIVIDE(total_errores_nf, total_puntos_saque) AS pct_errores_nf,
    SAFE_DIVIDE(total_subidas_red, total_puntos_saque) AS pct_sube_red,
    SAFE_DIVIDE(total_puntos_ganados_en_red, total_subidas_red) AS pct_efectividad_en_red,
    SAFE_DIVIDE(total_bp_convertidos, total_bp_oportunidades) AS pct_bp_convertidos,
    SAFE_DIVIDE(total_bp_fallados, total_bp_oportunidades) AS pct_bp_fallados,
    SAFE_DIVIDE(total_rally_corto, total_puntos_saque) AS pct_rally_corto,
    SAFE_DIVIDE(total_rally_largo, total_puntos_saque) AS pct_rally_largo,
    SAFE_DIVIDE(total_puntos_ganados, total_puntos_saque) AS pct_puntos_ganados_saque,
    SAFE_DIVIDE(rally_3_ganados, total_rally_3) AS pct_ganados_rally_3

FROM enriquecido
