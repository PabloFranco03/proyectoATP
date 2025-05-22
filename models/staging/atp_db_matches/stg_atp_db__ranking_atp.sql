{{ 
    config(
        materialized = 'incremental',
        unique_key = 'id_ranking'
    )
}}

WITH fuente AS (
    SELECT * 
    FROM {{ source('extra_ranking', 'ranking_atp') }}
),

casted AS (
    SELECT 
        ranking_date,
        player_id,
        TO_DATE(ranking_date, 'YYYYMMDD') AS ranking_fecha,
        rank AS posicion_ranking,
        points AS puntos,
        ingesta_tmz
    FROM fuente
    {% if is_incremental() %}
        WHERE ingesta_tmz > (SELECT MAX(ingesta_tmz) FROM {{ this }})
    {% endif %}
),

filtrado AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY player_id, ranking_date ORDER BY ingesta_tmz DESC) AS fila
    FROM casted
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ranking_date','player_id']) }} AS id_ranking,
    ranking_fecha,
    posicion_ranking,
    {{ dbt_utils.generate_surrogate_key(['player_id']) }} AS id_jugador,
    puntos,
    ingesta_tmz
FROM filtrado
WHERE fila = 1
