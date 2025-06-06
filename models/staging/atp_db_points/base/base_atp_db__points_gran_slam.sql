{{ config(
    materialized = 'view'
) }}

WITh raw AS (
    SELECT *
    from {{ source('extra_grand_slam', 'puntos_grand_slam') }}
    WHERE LEFT(SPLIT_PART(match_id, '-', 3), 1) != 2 
),

renamed AS(
    SELECT
        match_id,
        elapsed_time AS tiempo_transcurrido,
        set_no AS numero_set,
        p1_games_won AS p1_juegos_ganados,
        p2_games_won As p2_juegos_ganados,
        set_winner AS ganador_set,
        game_no AS numero_juego,
        game_winner AS ganador_juego,
        point_number AS num_punto_partido,
        point_winner As ganador_punto,
        point_server AS sacador,
        speed_kmh AS velocidad_saque_kmh,
        rally,
        p1_score,
        p2_score,
        p1_points_won AS puntos_ganados_p1,
        p2_points_won AS puntos_ganados_p2,
        p1_ace,
        p2_ace,
        p1_winner,
        p2_winner,
        p1_double_fault,
        p2_double_fault,
        p1_unf_err,
        p2_unf_err,
        p1_net_point,
        p2_net_point,
        p1_net_point_won,
        p2_net_point_won,
        p1_break_point,
        p2_break_point,
        p1_break_point_won,
        p2_break_point_won,
        p1_break_point_missed,
        p2_break_point_missed,
        serve_indicator AS indicador_saque,
        serve_number AS numero_saque,
        winner_type AS tipo_winner,
        winner_shot_type AS tipo_golpeo_winner,
        p1_distance_run AS distancia_recorrida_p1,
        p2_distance_run AS distancia_recorrida_p2,
        rally_count,
        serve_width AS lateral_saque ,
        serve_depth AS profundidad_saque,
        return_depth AS profundidad_resto,
        ingesta_tmz
    FROM raw
)

SELECT *
FROM renamed
