WITH raw_matches AS (
    SELECT *
    -- tourney_name
    FROM {{ source('atp', 'matches') }}
    WHERE {{ filtrado_copa_davis('tourney_level')}}
),

campos_torneo AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([limpiar_texto("tourney_name")]) }} AS id_torneo,
        tourney_name AS nombre,
    FROM raw_matches
)

SELECT DISTINCT *
FROM campos_torneo
order by nombre

