-- ============================================================
-- Consultas Athena sobre el Data Lake de PayTrack
--
-- Tablas disponibles (las crean los crawlers):
--   raw_transactions, raw_fraud_events             ← JSON, zona raw
--   processed_transactions, processed_fraud_events ← Parquet, zona processed
-- Revisa sus columnas y tipos en la consola de Glue antes de empezar:
-- no son iguales en ambas zonas.
--
-- Ejecuta con: python labs/02-data-lake/query_athena.py
-- No cambies las líneas "-- name:", el script las usa.
-- ============================================================

-- ── Parte 1: poda de particiones ─────────────────────────────
-- Monto total y número de transacciones aprobadas por categoría de comercio
-- en septiembre de 2026. Filtra usando las columnas de partición.
-- Escríbela una vez contra raw y otra contra processed.

-- name: q1_raw
-- TODO
;

-- name: q1_processed
-- TODO
;

-- ── Parte 2: formato columnar ────────────────────────────────
-- Tasa de fraude (%) por categoría de comercio sobre TODO el histórico
-- (sin filtro de partición). De nuevo contra raw y contra processed.
-- ¿Por qué cambia tanto el volumen escaneado si ambas leen todas las filas?

-- name: q2_raw
-- TODO
;

-- name: q2_processed
-- TODO
;

-- ── Parte 3: cruce entre datasets ────────────────────────────
-- Para cada patrón de fraude: número de eventos, risk_score promedio y
-- monto promedio de las transacciones asociadas. Usa solo la zona processed.

-- name: q3_fraud_patterns
-- TODO
;

-- ── Parte 4: zona curated ────────────────────────────────────
-- Crea con CTAS (CREATE TABLE AS SELECT) la tabla curated_daily_summary
-- con una fila por día y categoría de comercio: número de transacciones,
-- monto total, número de fraudes y tasa de aprobación (%).
-- Requisitos:
--   * formato Parquet, en s3://${BUCKET}/curated/daily_summary/
--   * particionada por year y month (en un CTAS las columnas de partición
--     deben ir al FINAL del SELECT)
-- Referencia: https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html

-- name: drop_daily_summary
DROP TABLE IF EXISTS curated_daily_summary;

-- name: ctas_daily_summary
-- TODO
;

-- name: q4_curated
-- Los 10 días con mayor monto total en la categoría 'travel', leyendo
-- solo la tabla curated. Compara el volumen escaneado con q1_processed.
-- TODO
;
