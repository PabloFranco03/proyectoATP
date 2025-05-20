WITH raw_matches AS (
    SELECT
    id_torneo_anio,
    id_torneo,
    id_superficie, 
    id_nivel_torneo,
    fecha_inicio,
    total_jugadores 
    FROM {{ ref('base_atp_db__matches') }}
),

campos_torneo_anio AS (
    SELECT DISTINCT
        id_torneo_anio,
        id_torneo,
        id_superficie,
        id_nivel_torneo,        
        fecha_inicio,
        EXTRACT(YEAR FROM fecha_inicio) AS anio_inicio,
        EXTRACT(MONTH FROM fecha_inicio) AS mes_inicio,
        total_jugadores 
    FROM raw_matches
)

SELECT 
    *
FROM campos_torneo_anio
