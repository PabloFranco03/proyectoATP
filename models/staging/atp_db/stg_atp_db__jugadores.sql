{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('atp', 'matches') }}
    WHERE {{ filtrado_copa_davis('tourney_level') }}
),

ganadores AS (
    SELECT DISTINCT
        winner_id AS id_jugador,
        winner_name AS nombre_jugador,
        winner_hand AS mano_dominante,
        winner_ht AS altura_cm,
        winner_ioc AS cod_pais
    FROM source
),

perdedores AS (
    SELECT DISTINCT
        loser_id AS id_jugador,
        loser_name AS nombre_jugador,
        loser_hand AS mano_dominante,
        loser_ht AS altura_cm,
        loser_ioc AS cod_pais
    FROM source
),

jugadores_union AS (
    SELECT * FROM ganadores
    UNION
    SELECT * FROM perdedores
),

registro_vacio AS (
    SELECT
        -1 AS id_jugador, 
        'Desconocido' AS nombre_jugador,
        NULL AS mano_dominante,
        NULL AS altura_cm,
        NULL AS cod_pais
),

ioc_paises AS (
    SELECT *
    FROM {{ ref('paises_ioc') }}
),

union_total AS (
    SELECT * FROM jugadores_union
    UNION ALL
    SELECT * FROM registro_vacio
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['id_jugador']) }} AS id_jugador,
    nombre_jugador,
    mano_dominante,
    altura_cm,
    cod_pais,
    p.pais_completo as pais_desc
FROM union_total j
LEFT JOIN ioc_paises p
    ON j.cod_pais = p.cod_ioc
