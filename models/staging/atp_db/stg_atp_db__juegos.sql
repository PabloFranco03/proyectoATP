{{ config(
    materialized = 'incremental',
    unique_key = 'id_partido || '-' || numero_set || '-' || numero_juego'
) }}

WITH puntos AS (
    SELECT
        match_id,
        numero_set,
        numero_juego,
        ganador_juego,
        sacador
    FROM {{ ref('base_extra_grand_slam__puntos_gran_slam') }}
    WHERE ganador_juego != 0
),

mapping AS (
    SELECT id_partido_otro, id_partido
    FROM {{ ref('int_match_id_mapping') }}
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
