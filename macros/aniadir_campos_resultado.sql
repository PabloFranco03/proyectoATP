{% macro aniadir_campos_resultado(score_column) %}
    -- Número de sets jugados (coincidencias tipo 6-4, 7-6, etc.)
    REGEXP_COUNT({{ score_column }}, '\\d+-\\d+') AS sets_jugados,

    -- Sets ganados por el ganador
    REGEXP_COUNT({{ score_column }}, '(^|\\s)6-|(^|\\s)7-') AS sets_ganador,

    -- Sets ganados por el perdedor
    (REGEXP_COUNT({{ score_column }}, '\\d+-\\d+') - REGEXP_COUNT({{ score_column }}, '(^|\\s)6-|(^|\\s)7-')) AS sets_perdedor,

    -- Número de tiebreaks jugados
    REGEXP_COUNT({{ score_column }}, '\\(\\d+\\)') AS tiebreaks_jugados,

    -- Si el último set fue un tiebreak
    CASE
        WHEN REGEXP_SUBSTR({{ score_column }}, '\\(\\d+\\)\\s*$', 1) IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS fue_tiebreak_final,

    -- Resultado normalizado
    REPLACE({{ score_column }}, ' ', ' / ') AS resultado_normalizado
{% endmacro %}
