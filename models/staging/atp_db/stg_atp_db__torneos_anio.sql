WITH raw_matches AS (
    SELECT 
    tourney_id,
    nombre_torneo, 
    id_superficie, 
    nivel_torneo,
    fecha_inicio,
    total_jugadores 
    FROM {{ source('atp', 'matches') }}
),

campos_torneo_anio AS (
    SELECT DISTINCT
        id_torneo_anio,
        id_torneo,
        id_superficie,
        id_nivel_torneo,        
        fecha_inicio,
        CAST(TO_CHAR(TO_DATE(fecha_inicio, 'YYYYMMDD'), 'YYYY') AS INT) AS anio_inicio,
        CAST(TO_CHAR(TO_DATE(fecha_inicio, 'YYYYMMDD'), 'MM') AS INT) AS mes_inicio,
        total_jugadores 
    FROM raw_matches
)

SELECT 
    *
FROM campos_torneo_anio
