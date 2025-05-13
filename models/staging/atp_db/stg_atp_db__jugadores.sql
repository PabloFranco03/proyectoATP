{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('atp', 'matches') }}
    WHERE {{ es_torneo_principal('tourney_level') }}
),

ganadores AS (
    SELECT DISTINCT
        winner_id       AS id_jugador,
        winner_name     AS nombre_jugador,
        winner_hand     AS mano_dominante,
        winner_ht       AS altura_cm,
        winner_ioc      AS cod_pais
    FROM source
    WHERE winner_id IS NOT NULL
),

perdedores AS (
    SELECT DISTINCT
        loser_id        AS id_jugador,
        loser_name      AS nombre_jugador,
        loser_hand      AS mano_dominante,
        loser_ht        AS altura_cm,
        loser_ioc       AS cod_pais
    FROM source
    WHERE loser_id IS NOT NULL
),

jugadores_union AS (
    SELECT * FROM ganadores
    UNION
    SELECT * FROM perdedores
),

registro_vacio AS (
    SELECT
        NULL    AS id_jugador,
        'Desconocido' AS nombre_jugador,
        NULL    AS mano_dominante,
        NULL    AS altura_cm,
        NULL    AS cod_pais
)

SELECT * FROM jugadores_union
UNION ALL
SELECT * FROM registro_vacio
