/*{{ config(
    materialized = 'incremental',
    unique_key = 'id_partido'
) }}*/

WITH source AS (
    SELECT *
    FROM {{ source('atp', 'matches') }}
    WHERE {{ es_torneo_principal('tourney_level') }}
    /*{% if is_incremental() %}
    AND TO_DATE(tourney_date, 'YYYYMMDD') >= (
        SELECT MAX(TO_DATE(tourney_date, 'YYYYMMDD')) FROM {{ this }}
    )
    {% endif %}*/
),

ganador AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['tourney_id', 'match_num']) }} AS id_partido,
        {{ dbt_utils.generate_surrogate_key(['winner_id']) }} AS id_jugador,
        TRUE AS ha_ganado,
        w_ace AS ace,
        w_df AS doble_falta,
        w_svpt AS puntos_saque,
        w_1stIn AS primeros_saques,
        w_1stWon AS puntos_ganados_1er,
        w_2ndWon AS puntos_ganados_2do,
        w_SvGms AS juegos_saque,
        w_bpSaved AS bp_salvados,
        w_bpFaced AS bp_enfrentados
    FROM source
),

perdedor AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['tourney_id', 'match_num']) }} AS id_partido,
        {{ dbt_utils.generate_surrogate_key(['loser_id']) }} AS id_jugador,
        FALSE AS ha_ganado,
        l_ace AS ace,
        l_df AS doble_falta,
        l_svpt AS puntos_saque,
        l_1stIn AS primeros_saques,
        l_1stWon AS puntos_ganados_1er,
        l_2ndWon AS puntos_ganados_2do,
        l_SvGms AS juegos_saque,
        l_bpSaved AS bp_salvados,
        l_bpFaced AS bp_enfrentados
    FROM source
),

estadisticas_union AS (
    SELECT * FROM ganador
    UNION ALL
    SELECT * FROM perdedor
),

registro_vacio AS (
    SELECT
        NULL AS id_partido,
        NULL AS id_jugador,
        NULL AS ha_ganado,
        NULL AS ace,
        NULL AS doble_falta,
        NULL AS puntos_saque,
        NULL AS primeros_saques,
        NULL AS puntos_ganados_1er,
        NULL AS puntos_ganados_2do,
        NULL AS juegos_saque,
        NULL AS bp_salvados,
        NULL AS bp_enfrentados
)

SELECT * FROM estadisticas_union
UNION ALL
SELECT * FROM registro_vacio
