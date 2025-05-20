--hacer bien incremental

{{ config(
    materialized = 'incremental',
    unique_key = ''
) }}

WITH puntos AS (
    SELECT
        match_id,
        numero_set,
        numero_juego,
        ganador_juego,
        sacador
    FROM {{ ref('base_atp_db__points_gran_slam') }}
    WHERE ganador_juego != 0
),

mapping AS (
    SELECT id_partido_otro, id_partido
    FROM {{ ref('int__union_ids') }}
)

SELECT
    m.id_partido,
    p.match_id,
    p.numero_set,
    p.numero_juego,
    MAX(p.ganador_juego) AS ganador_juego,
    MIN(p.sacador) AS sacador
FROM puntos p
LEFT JOIN mapping m
    ON p.match_id = m.id_partido_otro
GROUP BY m.id_partido, p.match_id, p.numero_set, p.numero_juego
