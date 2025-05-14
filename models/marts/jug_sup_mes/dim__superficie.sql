{{ config(
    materialized = 'table'
) }}

WITH dim_superficie AS (

    SELECT *
    FROM (
        VALUES
            ('Clay',   'Tierra batida', 'Lenta',       FALSE),
            ('Hard',   'Dura',          'Media',       TRUE),
            ('Grass',  'Hierba',        'Rápida',      TRUE)
    ) AS t (
        superficie,
        nombre_superficie,
        tipo_velocidad,
        es_rapida
    )

)

SELECT * FROM dim_superficie
