{{
    config(
        materialized = 'view'
    )
}}

WITH raw_torneos_info AS (
    SELECT  *
    FROM {{ source('torneos', 'torneos_info') }}
),

campos_info_torneo AS (
    SELECT
        {{ limpiar_texto('nombre_torneo') }} AS torneo_limpio, 
        nombre_torneo,
        TRIM(pais) AS pais,
        TRIM(ciudad) AS ciudad,
        primera_edicion,
        TRIM(estadio_principal) AS estadio_principal
    FROM raw_torneos_info
)

SELECT *,
    {{ dbt_utils.generate_surrogate_key(['torneo_limpio']) }} AS id_torneo
FROM campos_info_torneo

