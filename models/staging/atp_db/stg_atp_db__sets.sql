--hacer bien incremental

--hacer bien ctes

WITH puntos AS (
    SELECT
        match_id,
        numero_set,
        p1_juegos_ganados,
        p2_juegos_ganados,
        ganador_set
    FROM {{ ref('base_atp_db__puntos_gran_slam') }}
    WHERE ganador_set != 0
),

mapping AS (
    SELECT id_partido_otro, id_partido
    FROM {{ ref('int_match_id_mapping') }}
)

SELECT
    m.id_partido,
    p.match_id,
    p.numero_set,
    MAX(p.p1_juegos_ganados) AS p1_juegos_ganados,
    MAX(p.p2_juegos_ganados) AS p2_juegos_ganados,
    MAX(p.ganador_set) AS ganador_set,
    CASE
        WHEN MAX(p.ganador_set) = 1 THEN 2
        WHEN MAX(p.ganador_set) = 2 THEN 1
        ELSE NULL
    END AS perdedor_set,
    CONCAT(MAX(p.p1_juegos_ganados), '-', MAX(p.p2_juegos_ganados)) AS marcador_set
FROM puntos p
LEFT JOIN mapping m
    ON p.match_id = m.id_partido_otro
GROUP BY m.id_partido, p.match_id, p.numero_set
