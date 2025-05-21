WITH puntos_saque AS (
    SELECT *
    FROM {{ ref('fct__estadisticas_punto_jugador') }}
    WHERE numero_saque IS NOT NULL
),

resumen AS (
    SELECT

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

        SUM(CASE WHEN numero_saque = 1 THEN 1 ELSE 0 END) AS total_saques_1,
        SUM(CASE WHEN numero_saque = 2 THEN 1 ELSE 0 END) AS total_saques_2,

        SUM(CASE WHEN ha_ganado THEN 1 ELSE 0 END) AS total_puntos_ganados,
        
        -- Medias
        AVG(CASE WHEN numero_saque = 1 AND velocidad_saque > 0 THEN velocidad_saque END) AS vel_media_1er_saque,
        AVG(CASE WHEN numero_saque = 2 AND velocidad_saque > 0 THEN velocidad_saque END) AS vel_media_2do_saque,
        AVG(distancia_recorrida) AS media_distancia_recorrida,
        AVG(rally_count) AS media_rally_count,

        id_jugador,
        id_torneo_anio

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
        j.nombre_jugador,
        ta.anio_inicio AS anio,
        s.nombre_superficie,
        nt.nivel_torneo,
        r.*,
        s.es_rapida
    FROM resumen r
    LEFT JOIN jugadores j ON r.id_jugador = j.id_jugador
    LEFT JOIN torneo_anio ta ON r.id_torneo_anio = ta.id_torneo_anio
    LEFT JOIN superficie s ON ta.id_superficie = s.id_superficie
    LEFT JOIN nivel_torneo nt ON ta.id_nivel_torneo = nt.id_nivel_torneo
)

SELECT
    *,
    -- Porcentajes y ratios
    CASE WHEN total_saques_1 = 0 THEN NULL ELSE total_aces / total_saques_1 END AS pct_aces_por_1er_saque,
    CASE WHEN total_saques_2 = 0 THEN NULL ELSE total_dobles_faltas / total_saques_2 END AS pct_df_por_2do_saque,
    CASE WHEN total_puntos_saque = 0 THEN NULL ELSE total_winners / total_puntos_saque END AS pct_winners,
    CASE WHEN total_puntos_saque = 0 THEN NULL ELSE total_errores_nf / total_puntos_saque END AS pct_errores_nf,
    CASE WHEN total_puntos_saque = 0 THEN NULL ELSE total_subidas_red / total_puntos_saque END AS pct_sube_red,
    CASE WHEN total_subidas_red = 0 THEN NULL ELSE total_puntos_ganados_en_red / total_subidas_red END AS pct_efectividad_en_red,
    CASE WHEN total_puntos_saque = 0 THEN NULL ELSE total_rally_corto / total_puntos_saque END AS pct_rally_corto,
    CASE WHEN total_puntos_saque = 0 THEN NULL ELSE total_rally_largo / total_puntos_saque END AS pct_rally_largo,
    CASE WHEN total_puntos_saque = 0 THEN NULL ELSE total_puntos_ganados / total_puntos_saque END AS pct_puntos_ganados_saque,

FROM enriquecido

where anio is not null
