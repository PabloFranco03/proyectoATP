{{ comparar_flag_con_logica(
    'fct__estadisticas_punto_jugador',
    'ha_ganado',
    'CASE WHEN ace = TRUE THEN TRUE ELSE ha_ganado END'
) }}