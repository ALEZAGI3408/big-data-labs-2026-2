-- ============================================================
-- Las mismas consultas de negocio sobre el esquema estrella
--
-- Reescribe cada consulta de oltp_problems.sql usando SOLO tablas dw.*
-- Reglas (las valida check_queries.py):
--   * Mantén el EXPLAIN ANALYZE delante de cada SELECT.
--   * Devuelve las mismas columnas, con los mismos nombres y en el mismo
--     orden que la versión OLTP: los resultados deben ser idénticos.
--   * Usa las dimensiones para filtrar y agrupar: nada de EXTRACT() ni
--     funciones de fecha sobre la tabla de hechos.
-- Después compara los planes de ejecución con los de la versión OLTP.
-- ============================================================

-- Q1: Monto total por categoría de comercio (últimos 30 días)
-- TODO

;

-- Q2: Tasa de fraude por segmento de cliente
-- Pista: dim_customer es SCD2. ¿Debes filtrar por is_current? ¿Por qué?
-- TODO

;

-- Q3: Volumen de transacciones por día de la semana y hora (últimos 7 días)
-- TODO

;
