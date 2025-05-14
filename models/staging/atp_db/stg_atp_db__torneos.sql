{{ config(materialized='view') }}

WITH base AS (
    SELECT *
    FROM {{ ref('base_atp_db__torneos') }}
)

SELECT
    id_torneo,
    nombre,
    superficie,
    nivel,
    sets_maximos
FROM base
