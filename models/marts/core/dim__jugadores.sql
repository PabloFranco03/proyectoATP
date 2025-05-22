WITH jugadores AS (
    SELECT * 
    FROM {{ ref('stg_atp_db__jugadores') }}
),

renamed_casted AS (
    SELECT
        id_jugador,
        nombre_jugador,
        mano_dominante,
        CASE 
            WHEN mano_dominante = 'R' THEN 'Diestro'
            WHEN mano_dominante = 'L' THEN 'Zurdo'
            WHEN mano_dominante = 'U' THEN 'Desconocido'
            WHEN mano_dominante = 'A' THEN 'Ambidiestro'
        END AS mano_desc,
        altura_cm,
        cod_pais,
        pais_desc,
        fecha_nacimiento,
        wikidata_id
    FROM jugadores
)

SELECT * FROM renamed_casted
