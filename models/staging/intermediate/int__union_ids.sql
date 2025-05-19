{{ config(materialized = 'table') }}

WITH partidos AS (
    SELECT
        m.id_partido,
        m.match_id_unificada,
        m.id_ganador,
        m.id_perdedor,
        b1.match_id AS match_id_gran_slam_gan1,
        b2.match_id AS match_id_gran_slam_gan2,
        b1.match_id_unificada_gan1,
        b2.match_id_unificada_gan2,
        b1.player1 AS player1_b1,
        b2.player2 AS player2_b2
    FROM {{ ref('base_atp_db__matches') }} m
    LEFT JOIN {{ ref('base_atp_db__matches_gran_slam') }} b1
        ON m.match_id_unificada = b1.match_id_unificada_gan1
    LEFT JOIN {{ ref('base_atp_db__matches_gran_slam') }} b2
        ON m.match_id_unificada = b2.match_id_unificada_gan2
)

SELECT
    COALESCE(match_id_gran_slam_gan1, match_id_gran_slam_gan2) AS id_partido_otro,
    id_partido,

    CASE 
        WHEN match_id_unificada = match_id_unificada_gan1 THEN player1_b1
        WHEN match_id_unificada = match_id_unificada_gan2 THEN player2_b2
        ELSE NULL 
    END AS ganador,

    CASE 
        WHEN match_id_unificada = match_id_unificada_gan1 THEN id_ganador
        WHEN match_id_unificada = match_id_unificada_gan2 THEN id_perdedor
        ELSE NULL 
    END AS id_player1,

    CASE 
        WHEN match_id_unificada = match_id_unificada_gan1 THEN id_perdedor
        WHEN match_id_unificada = match_id_unificada_gan2 THEN id_ganador
        ELSE NULL 
    END AS id_player2

FROM partidos
WHERE COALESCE(match_id_gran_slam_gan1, match_id_gran_slam_gan2) IS NOT NULL
