{{ comparar_flag_con_logica(
    'fct__estadisticas_punto_jugador',
    'ha_ganado',
    'CASE WHEN doble_falta = TRUE THEN FALSE ELSE ha_ganado END'
) }}
