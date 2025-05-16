WITH raw_matches AS (
    SELECT *
    FROM {{ source('atp', 'matches') }}
    WHERE {{ filtrado_copa_davis('tourney_level')}}
),

campos_torneo_anio AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['tourney_id']) }} AS id_torneo_anio,
        {{ dbt_utils.generate_surrogate_key([limpiar_texto("tourney_name")]) }} AS id_torneo,
        {{ dbt_utils.generate_surrogate_key(['surface']) }} AS id_superficie,
        {{ dbt_utils.generate_surrogate_key(['tourney_level']) }} AS id_nivel_torneo,        
        best_of AS sets_maximos, 
        TO_DATE(tourney_date, 'YYYYMMDD') AS fecha_inicio,
        CAST(TO_CHAR(TO_DATE(tourney_date, 'YYYYMMDD'), 'YYYY') AS INT) AS anio_inicio,
        CAST(TO_CHAR(TO_DATE(tourney_date, 'YYYYMMDD'), 'MM') AS INT) AS mes_inicio,
        draw_size as total_jugadores 
    FROM raw_matches
)

SELECT 
    *
FROM campos_torneo_anio
