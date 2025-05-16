WITH torneo AS (
    SELECT *
    FROM {{ ref('stg_atp_db__torneos') }}
)

SELECT
    id_torneo,
    nombre,
    pais,
    ciudad,
    primera_edicion,
    estadio_principal
FROM torneo
