-- {{ config(materialized='table') }}

WITH torneo_base AS (
    SELECT *
    FROM {{ ref('base_atp_db__torneos') }}
),

torneos_info_limpio AS (
    SELECT *
    FROM {{ ref('base_atp_db__torneos_info') }}
)

SELECT
    b.id_torneo,
    b.nombre,
    i.pais,
    i.ciudad,
    i.primera_edicion,
    i.estadio_principal
FROM torneo_base b
LEFT JOIN torneos_info_limpio i
    ON b.id_torneo = i.id_torneo
