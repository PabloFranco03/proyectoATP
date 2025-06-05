-- tests/test_marcador_fin_set_valido.sql
SELECT *
FROM {{ ref('nombre_modelo') }}
WHERE ganador_set_juegos > perdedor_set_juegos
