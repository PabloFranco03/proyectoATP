{{
    config(
        materialized = 'view'
    )
}}

WITH raw AS (
    SELECT *
    FROM {{ source('extra_grand_slam', 'matches_grand_slam') }}
    WHERE LEFT(CAST(match_num AS STRING), 1) = '1' 

),

casted AS (
    SELECT
        match_id,
        CAST(year AS INT) AS year,
        {{ limpiar_texto('slam') }} AS slam_limpio,
        CAST(match_num AS INT) AS match_num,
        {{ limpiar_texto('player1') }} AS player1_limpio,
        {{ limpiar_texto('player2') }} AS player2_limpio,
        player1,
        player2,
        ingesta_tmz
    FROM raw
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['year','slam_limpio','player1_limpio','player2_limpio']) }} AS match_id_unificada_gan1,
    {{ dbt_utils.generate_surrogate_key(['year','slam_limpio','player2_limpio','player1_limpio']) }} AS match_id_unificada_gan2,
    match_id,
    player1,
    player2,
    ingesta_tmz
FROM casted
