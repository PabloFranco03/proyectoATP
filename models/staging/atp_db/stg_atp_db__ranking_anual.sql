WITH source AS (
    SELECT *
    FROM {{ source('atp', 'matches') }}
    WHERE {{ filtrado_copa_davis('tourney_level') }}

),

ganadores AS (
    SELECT
        winner_id AS id_jugador,
        winner_rank_points AS puntos,
        winner_age AS edad,
        TO_DATE(TO_VARCHAR(tourney_date), 'YYYYMMDD') AS fecha
    FROM source
),

perdedores AS (
    SELECT
        loser_id AS id_jugador,
        loser_rank_points AS puntos,
        loser_age AS edad,
        TO_DATE(TO_VARCHAR(tourney_date), 'YYYYMMDD') AS fecha
    FROM source
),

union_estadisticas AS (
    SELECT * FROM ganadores
    UNION ALL
    SELECT * FROM perdedores
),

con_anio AS (
    SELECT
        *,
        EXTRACT(YEAR FROM fecha) AS anio
    FROM union_estadisticas
),

ultimos_partidos AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY id_jugador, anio ORDER BY fecha DESC) AS fila
        FROM con_anio
    )
    WHERE fila = 1 AND puntos IS NOT NULL
),

ranking_recalculado AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['id_jugador','anio']) }} AS id_ranking,
        {{ dbt_utils.generate_surrogate_key(['id_jugador']) }} AS id_jugador,
        RANK() OVER (PARTITION BY anio ORDER BY puntos DESC) AS ranking_final,
        puntos AS puntos_finales,
        anio,
        edad,
        fecha
    FROM ultimos_partidos
)

SELECT * FROM ranking_recalculado
