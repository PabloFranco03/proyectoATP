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
        {{ dbt_utils.generate_surrogate_key([limpiar_texto("nombre_torneo")]) }} AS id_torneo,
        nombre_torneo,
        pais,
        ciudad,
        primera_edicion,
        estadio_principal
    FROM raw_torneos_info
)

SELECT *
FROM campos_info_torneo

