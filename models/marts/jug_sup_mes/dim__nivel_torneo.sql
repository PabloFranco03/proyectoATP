{{ config(
    materialized = 'table'
) }}

WITH dim_nivel_torneo AS (

    SELECT *
    FROM (
        VALUES
            ('G', 'Grand Slam', 2000),
            ('F', 'ATP Finals', 1500),
            ('M', 'Masters 1000', 1000),
            ('A', 'ATP 500', 500),
            ('C', 'ATP 250', 250)
    ) AS t (
        nivel,
        descripcion,
        puntos_ganador
    )

)

SELECT * FROM dim_nivel_torneo
