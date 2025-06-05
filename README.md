# 🎾 Proyecto Final ATP – Ingeniería de Datos

Este proyecto fue desarrollado como trabajo final para el curso de Ingeniería de Datos.  
El objetivo principal fue analizar cómo ha influido el **saque** en el rendimiento de los jugadores de tenis en los últimos 20 años, dentro del circuito profesional masculino ATP.

---

## 🧠 Objetivo

Demostrar la capacidad para construir un pipeline de datos completo desde cero, integrando diferentes fuentes, aplicando buenas prácticas de modelado, y generando una tabla de hechos lista para análisis avanzado.

---

## 📦 Datasets utilizados

Se integraron dos fuentes principales:

1. **Dataset completo de partidos ATP** (histórico desde el año 2000).
2. **Dataset punto a punto de torneos Grand Slam**, con detalle de cada jugada.

Ambos datasets requerían estructuras distintas, por lo que fue necesario:

- Normalizar campos.
- Unificar convenciones de nombres.
- Crear claves comunes (`match_id`, `player_id`, etc.).
- Limpiar registros inconsistentes.
- Construir modelos escalables con dbt.

---

## 🛠️ Tecnologías utilizadas

- **Snowflake** (almacenamiento y consultas)
- **dbt** (transformaciones y modelado en capas: bronze, silver y gold)
- **SQL** (consultas complejas y lógica de negocio)

---

## 📊 Resultado

Aunque no se construyó un dashboard final, el proyecto generó una **tabla de hechos consolidada** que permite responder preguntas como:

- ¿Ha aumentado la efectividad del saque con los años?
- ¿Qué superficies favorecen más el servicio?
- ¿Hay diferencias en la duración de rallies según quién saca?

---

## 🚧 Retos y aprendizajes

- El mayor reto fue **crear claves unificadas** entre datasets distintos.
- Aprendí a realizar un proyecto completo de forma **individual**, desde la ingestión de datos hasta la capa de explotación.
- También reforcé buenas prácticas con `dbt`, diseño modular y control de calidad.

---

## 📌 Estado

Proyecto finalizado. Listo para análisis o visualización en herramientas como Power BI o Tableau. Alguna tabla de hechos sin terminar.


