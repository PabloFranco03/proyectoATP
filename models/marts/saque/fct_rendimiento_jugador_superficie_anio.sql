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
),

superficie AS (
    SELECT id_superficie, nombre_superficie
    FROM {{ ref('dim__superficie') }}
),

todos AS (
SELECT
    r.*,
    j.altura_cm,
    rk.posicion_ranking,
    s.nombre_superficie,

    -- Métricas derivadas
    CASE WHEN total_puntos_saque != 0 THEN total_primeros_saques / total_puntos_saque ELSE 0 END AS pct_primeros_saques_adentro,
    CASE WHEN (total_puntos_saque - total_primeros_saques) != 0 THEN (total_puntos_saque - total_primeros_saques) / total_puntos_saque ELSE 0 END AS pct_segundos_saques_jugados,
    CASE WHEN total_primeros_saques != 0 THEN total_puntos_ganados_1er / total_primeros_saques ELSE 0 END AS pct_puntos_ganados_1er,
    CASE WHEN (total_puntos_saque - total_primeros_saques) != 0 THEN total_puntos_ganados_2do / (total_puntos_saque - total_primeros_saques) ELSE 0 END AS pct_puntos_ganados_2do,
    CASE WHEN partidos_disputados != 0 THEN partidos_ganados / partidos_disputados ELSE 0 END AS pct_partidos_ganados,
    CASE WHEN total_bp_enfrentados != 0 THEN total_bp_salvados / total_bp_enfrentados ELSE 0 END AS pct_break_points_salvados,
    CASE WHEN partidos_disputados != 0 THEN total_juegos_saque / partidos_disputados ELSE 0 END AS juegos_saque_por_partido,
    CASE WHEN partidos_disputados != 0 THEN total_aces / partidos_disputados ELSE 0 END AS aces_por_partido,
    CASE WHEN total_juegos_saque != 0 THEN total_aces / total_juegos_saque ELSE 0 END AS aces_por_juego_saque,
    CASE WHEN total_primeros_saques != 0 THEN total_aces / total_primeros_saques ELSE 0 END AS pct_aces_por_primer_saque,
    CASE WHEN total_puntos_saque != 0 THEN total_aces / total_puntos_saque ELSE 0 END AS pct_aces_sobre_total_puntos,
    CASE WHEN partidos_disputados != 0 THEN total_dobles_faltas / partidos_disputados ELSE 0 END AS dobles_faltas_por_partido,
    CASE WHEN (total_puntos_saque - total_primeros_saques) != 0 THEN total_dobles_faltas / (total_puntos_saque - total_primeros_saques) ELSE 0 END AS pct_dobles_faltas_sobre_2dos_saques,
    CASE WHEN total_dobles_faltas != 0 THEN total_aces / total_dobles_faltas ELSE 0 END AS ratio_aces_dobles_faltas,

FROM resumen r
LEFT JOIN jugadores j ON r.id_jugador = j.id_jugador
LEFT JOIN ranking_final_anio rk ON r.id_jugador = rk.id_jugador AND r.anio = rk.anio
LEFT JOIN superficie s ON r.id_superficie = s.id_superficie
)

SELECT *
FROM todos 

