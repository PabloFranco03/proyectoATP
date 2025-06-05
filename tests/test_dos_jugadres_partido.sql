-- tests/test_dos_jugadores_por_partido.sql
SELECT id_partido
FROM {{ ref('fct__estadisticas_jugador_partido') }}
GROUP BY id_partido
HAVING COUNT(*) != 2