WITH superficie AS (
    SELECT *
    FROM {{ ref('stg__nivel_torneo') }}
)

SELECT
    id_nivel_torneo,
    nivel_torneo,
    nivel_torneo_desc
FROM superficie
