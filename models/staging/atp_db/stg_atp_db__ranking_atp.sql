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
        {{ dbt_utils.generate_surrogate_key(['ranking_date','player_id']) }} AS id_ranking,
        TO_DATE(ranking_date, 'YYYYMMDD') AS ranking_fecha,
        rank AS posicion,
        {{ dbt_utils.generate_surrogate_key(['player_id']) }} AS id_jugador,
        points AS puntos,
        ingesta_tmz
    FROM fuente
    {% if is_incremental() %}
        WHERE ingesta_tmz > (SELECT MAX(ingesta_tmz) FROM {{ this }})
    {% endif %}
)

SELECT * FROM casted
