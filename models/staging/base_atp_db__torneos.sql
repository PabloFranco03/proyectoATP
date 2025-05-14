WITH raw_matches AS (
    SELECT *
    FROM {{ source('atp', 'matches') }}
    WHERE {{ es_torneo_principal('tourney_level') }}
),

campos_torneo AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['tourney_name']) }} as id_torneo,
        tourney_name AS nombre,
        surface AS superficie,
        tourney_level AS nivel,
        best_of AS sets_maximos
    FROM raw_matches
)

SELECT DISTINCT *
FROM campos_torneo

