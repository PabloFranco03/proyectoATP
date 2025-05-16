{{ config(materialized='table') }}

WITH raw_matches AS (
    SELECT *
    FROM {{ source('atp', 'matches') }}
    WHERE {{ filtrado_copa_davis('tourney_level') }}
)

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['tourney_level']) }} AS id_nivel_torneo,
    tourney_level AS nivel_torneo,

    CASE nivel_torneo
        WHEN 'A' THEN 'Torneo ATP'
        WHEN 'G' THEN 'Grand Slam'
        WHEN 'O' THEN 'Olimpiadas'
        WHEN 'F' THEN 'Next Gen Finals'
        WHEN 'M' THEN 'Master 1000'
        ELSE 'Desconocida'
    END AS nivel_torneo_desc,


FROM raw_matches