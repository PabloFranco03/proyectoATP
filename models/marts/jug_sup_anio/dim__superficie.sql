WITH superficie AS (
    SELECT *
    FROM {{ ref('stg__superficie') }}
)

SELECT
    id_superficie,
    superficie,
    nombre_superficie,
    es_rapida
FROM superficie
