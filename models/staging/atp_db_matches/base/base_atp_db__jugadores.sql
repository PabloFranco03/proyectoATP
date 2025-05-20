{{
    config(
        materialized = 'view'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('extra_jugadores', 'jugadores') }}
),

limpio AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['player_id']) }} AS id_jugador,
        name_first AS nombre,
        name_last AS apellido,
        CONCAT_WS(' ', name_first, name_last) AS nombre_completo,
        hand AS mano_dominante,
        TRY_TO_DATE(dob, 'YYYYMMDD') AS fecha_nacimiento,
        ioc AS cod_pais,
        height AS altura_cm,
        wikidata_id,
        ingesta_tmz
    FROM source
)

SELECT *
FROM limpio
