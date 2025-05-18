-- models/base/bronze/base_puntos_grand_slam.sql

WITH raw AS(
    SELECT *
    FROM {{ source('extra_grand_slam', 'puntos_grand_slam') }}
)


fuente AS (
    SELECT
        match_id,
        elapsed_time,
        CAST(set_no AS INT) AS set_no,
        CAST(p1_games_won AS INT) AS p1_games_won,
        CAST(p2_games_won AS INT) AS p2_games_won,
        CAST(set_winner AS INT) AS set_winner,
        CAST(game_no AS INT) AS game_no,
        CAST(game_winner AS INT) AS game_winner,
        point_number,
        CAST(point_winner AS INT) AS point_winner,
        CAST(point_server AS INT) AS point_server,
        CAST(NULLIF(speed_kmh, '') AS FLOAT) AS speed_kmh,
        rally,
        p1_score,
        p2_score,
        CAST(p1_momentum AS INT) AS p1_momentum,
        CAST(p2_momentum AS INT) AS p2_momentum,
        CAST(p1_points_won AS INT) AS p1_points_won,
        CAST(p2_points_won AS INT) AS p2_points_won,
        CAST(p1_ace AS INT) AS p1_ace,
        CAST(p2_ace AS INT) AS p2_ace,
        CAST(p1_winner AS INT) AS p1_winner,
        CAST(p2_winner AS INT) AS p2_winner,
        CAST(p1_double_fault AS INT) AS p1_double_fault,
        CAST(p2_double_fault AS INT) AS p2_double_fault,
        CAST(p1_unf_err AS INT) AS p1_unf_err,
        CAST(p2_unf_err AS INT) AS p2_unf_err,
        CAST(p1_net_point AS INT) AS p1_net_point,
        CAST(p2_net_point AS INT) AS p2_net_point,
        CAST(p1_net_point_won AS INT) AS p1_net_point_won,
        CAST(p2_net_point_won AS INT) AS p2_net_point_won,
        CAST(p1_break_point AS INT) AS p1_break_point,
        CAST(p2_break_point AS INT) AS p2_break_point,
        CAST(p1_break_point_won AS INT) AS p1_break_point_won,
        CAST(p2_break_point_won AS INT) AS p2_break_point_won,
        CAST(p1_first_srv_in AS INT) AS p1_first_srv_in,
        CAST(p2_first_srv_in AS INT) AS p2_first_srv_in,
        CAST(p1_first_srv_won AS INT) AS p1_first_srv_won,
        CAST(p2_first_srv_won AS INT) AS p2_first_srv_won,
        CAST(p1_second_srv_in AS INT) AS p1_second_srv_in,
        CAST(p2_second_srv_in AS INT) AS p2_second_srv_in,
        CAST(p1_second_srv_won AS INT) AS p1_second_srv_won,
        CAST(p2_second_srv_won AS INT) AS p2_second_srv_won,
        CAST(p1_forced_error AS INT) AS p1_forced_error,
        CAST(p2_forced_error AS INT) AS p2_forced_error,
        history,
        CAST(NULLIF(speed_mph, '') AS FLOAT) AS speed_mph,
        CAST(p1_break_point_missed AS INT) AS p1_break_point_missed,
        CAST(p2_break_point_missed AS INT) AS p2_break_point_missed,
        serve_indicator,
        serve_direction,
        CAST(winner_fh AS INT) AS winner_fh,
        CAST(winner_bh AS INT) AS winner_bh,
        serving_to,
        CAST(p1_turning_point AS INT) AS p1_turning_point,
        CAST(p2_turning_point AS INT) AS p2_turning_point,
        CAST(serve_number AS INT) AS serve_number,
        winner_type,
        winner_shot_type,
        CAST(p1_distance_run AS FLOAT) AS p1_distance_run,
        CAST(p2_distance_run AS FLOAT) AS p2_distance_run,
        CAST(rally_count AS INT) AS rally_count,
        serve_width,
        serve_depth,
        return_depth,
        ingesta_tmz
    FROM raw
)

SELECT *
FROM fuente
