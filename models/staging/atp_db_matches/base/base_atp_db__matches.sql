{{
    config(
        materialized = 'view'
    )
}}

with source as (
    select *
    from {{ source('atp', 'matches') }}
    where {{ filtrado_copa_davis_y_finals('tourney_level') }}
),

partidos AS (
    SELECT

        {{ dbt_utils.generate_surrogate_key(['tourney_id']) }} AS id_torneo_anio,
        {{ dbt_utils.generate_surrogate_key(['tourney_id', 'match_num', 'winner_id']) }} AS id_partido_estadisticas_gan,
        {{ dbt_utils.generate_surrogate_key(['tourney_id', 'match_num', 'loser_id']) }} AS id_partido_estadisticas_per,
        {{ dbt_utils.generate_surrogate_key(['tourney_id', 'match_num']) }} AS id_partido,
        {{ limpiar_texto('tourney_name') }} AS torneo_limpio,
        tourney_name AS nombre_torneo,
        {{ limpiar_texto('surface') }} AS superficie_limpia,
        surface AS superficie,
        draw_size as total_jugadores,
        {{ dbt_utils.generate_surrogate_key(['tourney_level']) }} AS id_nivel_torneo,
        tourney_level AS nivel_torneo,
        TO_DATE(tourney_date, 'YYYYMMDD') AS fecha_inicio,
        match_num as numero_partido_torneo,
        {{ dbt_utils.generate_surrogate_key(['winner_id']) }} AS id_ganador,
        WINNER_NAME,
        WINNER_HAND,
        WINNER_HT,
        WINNER_IOC,
        WINNER_AGE,
        {{ dbt_utils.generate_surrogate_key(['loser_id']) }} AS id_perdedor,
        LOSER_NAME,
        LOSER_HAND,
        LOSER_HT,
        LOSER_IOC,
        LOSER_AGE,
        score AS resultado,
        best_of AS sets_maximos,
        {{ dbt_utils.generate_surrogate_key(['round']) }} AS id_ronda_torneo,
        round AS ronda_torneo,
        minutes AS duracion_minutos,
        W_ACE,
        W_DF,
        W_SVPT,
        W_1STIN,
        W_1STWON,
        W_2NDWON,
        W_SVGMS,
        W_BPSAVED,
        W_BPFACED,
        L_ACE,
        L_DF,
        L_SVPT,
        L_1STIN,
        L_1STWON,
        L_2NDWON,
        L_SVGMS,
        L_BPSAVED,
        L_BPFACED,
        WINNER_RANK,
        WINNER_RANK_POINTS,
        LOSER_RANK,
        LOSER_RANK_POINTS,
        INGESTA_TMZ,
        CAST(TO_CHAR(TO_DATE(tourney_date, 'YYYYMMDD'), 'YYYY') AS INT) AS year,
        {{ limpiar_texto('winner_name') }} AS player1_limpio,
        {{ limpiar_texto('loser_name') }} AS player2_limpio

    FROM source
)

SELECT 
    *,
    {{ dbt_utils.generate_surrogate_key(['torneo_limpio']) }} AS id_torneo,
    {{ dbt_utils.generate_surrogate_key(['superficie_limpia']) }} AS id_superficie,
    {{ dbt_utils.generate_surrogate_key(['year','torneo_limpio','player1_limpio','player2_limpio']) }} AS match_id_unificada
FROM partidos


