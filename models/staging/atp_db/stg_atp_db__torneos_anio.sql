WITH raw_matches AS (
    SELECT *
    FROM {{ source('atp', 'matches') }}
    WHERE {{ es_torneo_principal('tourney_level') }}
),

campos_torneo_anio AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['tourney_id']) }} AS id_torneo_anio,
        {{ dbt_utils.generate_surrogate_key(['tourney_name']) }} AS id_torneo,
        TO_DATE(tourney_date, 'YYYYMMDD') AS fecha_inicio,
        TO_CHAR(TO_DATE(tourney_date, 'YYYYMMDD'), 'YYYY') AS anio_inicio,
        TO_CHAR(TO_DATE(tourney_date, 'YYYYMMDD'), 'MM') AS mes_inicio,
        draw_size as total_jugadores
    FROM raw_matches
)

SELECT *
FROM campos_torneo_anio
