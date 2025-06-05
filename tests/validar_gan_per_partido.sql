-- depends_on: {{ ref('fct__estadisticas_jugador_partido') }}

{{ validar_binaria_por_partido('fct__estadisticas_jugador_partido', 'ha_ganado', 'id_partido') }}
