{{ config(materialized='table') }}

WITH raw_matches AS (
    SELECT *
    FROM {{ source('atp', 'matches') }}
    WHERE {{ filtrado_copa_davis('tourney_level') }}
)

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['round']) }} AS id_ronda_torneo,
    round AS ronda_torneo,

    CASE round
        WHEN 'F'     THEN 'Final'
        WHEN 'SF'    THEN 'Semifinal'
        WHEN 'QF'    THEN 'Cuartos de final'
        WHEN 'R16'   THEN 'Octavos de final'
        WHEN 'R32'   THEN 'Dieciseisavos de final'
        WHEN 'R64'   THEN 'Treintaidosavos de final'
        WHEN 'R128'  THEN 'Sesentaicuatroavos de final'
        WHEN 'RR'    THEN 'Round Robin'
        WHEN 'BR'    THEN 'Partido por el 3er puesto'
        ELSE 'Ronda desconocida'
    END AS ronda_torneo_desc

FROM raw_matches
