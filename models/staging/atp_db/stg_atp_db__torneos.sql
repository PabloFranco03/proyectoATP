{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('atp', 'matches') }}
    WHERE {{ es_torneo_principal('tourney_level') }}
),

torneos AS (
    SELECT DISTINCT
        tourney_id             AS id_torneo,
        tourney_name           AS nombre_torneo,
        surface                AS superficie,
        draw_size              AS tamano_cuadro,
        tourney_level          AS nivel_torneo,
        best_of                AS sets_maximos,
        TO_DATE(TO_VARCHAR(tourney_date), 'YYYYMMDD') AS fecha_inicio,
        CAST(TO_CHAR(TO_DATE(TO_VARCHAR(tourney_date), 'YYYYMMDD'), 'YYYY') AS INT) AS anio_inicio,
        CAST(TO_CHAR(TO_DATE(TO_VARCHAR(tourney_date), 'YYYYMMDD'), 'MM') AS INT) AS mes_inicio
    FROM source
),

registro_vacio AS (
    SELECT
        NULL AS id_torneo,
        'Desconocido' AS nombre_torneo,
        NULL AS superficie,
        NULL AS tamano_cuadro,
        NULL AS nivel_torneo,
        NULL AS sets_maximos,
        NULL AS fecha_inicio,
        NULL AS anio_inicio,
        NULL AS mes_inicio
)

SELECT * FROM torneos
UNION ALL
SELECT * FROM registro_vacio
