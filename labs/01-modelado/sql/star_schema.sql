-- ============================================================
-- PayTrack Star Schema (OLAP)
-- Esquema estrella para análisis de transacciones
--
-- Las dimensiones de calendario (dim_date, dim_time) están dadas.
-- Tú diseñas el resto: dimensiones de negocio, tabla de hechos e índices.
--
-- Contrato mínimo (lo usan load_star.py y check_queries.py):
--   * Cada dimensión tiene una surrogate key llamada <dimensión>_key
--     (customer_key, merchant_key, payment_method_key) y conserva su
--     natural key del OLTP (customer_id, merchant_id, method_name).
--   * dim_payment_method tiene la columna method_type.
--   * dim_customer es SCD Tipo 2 con valid_from, valid_to e is_current.
--   * fact_transactions tiene transaction_id, las 5 foreign keys
--     (date_key, time_key, customer_key, merchant_key, payment_method_key)
--     y las medidas amount_cop, is_fraud, is_approved y fraud_risk_score.
-- Todo lo demás (tipos de dato, atributos descriptivos, restricciones,
-- índices) es decisión de diseño tuya y debes justificarlo en el informe.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS dw;

-- ── Dimensiones de calendario (dadas) ──────────────────────

-- Dim: Fecha (granularidad: día)
CREATE TABLE dw.dim_date (
    date_key        INT          PRIMARY KEY,  -- YYYYMMDD
    full_date       DATE         NOT NULL,
    year            SMALLINT     NOT NULL,
    quarter         SMALLINT     NOT NULL,
    month           SMALLINT     NOT NULL,
    month_name      VARCHAR(20)  NOT NULL,
    week_of_year    SMALLINT     NOT NULL,
    day_of_month    SMALLINT     NOT NULL,
    day_of_week     SMALLINT     NOT NULL,     -- 0 = domingo … 6 = sábado
    day_name        VARCHAR(20)  NOT NULL,
    is_weekend      BOOLEAN      NOT NULL,
    is_holiday_co   BOOLEAN      NOT NULL DEFAULT FALSE
);

-- Dim: Hora (granularidad: minuto del día)
CREATE TABLE dw.dim_time (
    time_key        INT          PRIMARY KEY,  -- HHMM
    hour            SMALLINT     NOT NULL,
    minute          SMALLINT     NOT NULL,
    period_of_day   VARCHAR(20)  NOT NULL      -- morning, afternoon, evening, night
);

-- ── TODO 1: Dimensión cliente (SCD Tipo 2) ─────────────────
-- Atributos analíticos: ciudad, segmento, rango de ingreso y antigüedad.
-- NO llegan al DW: name y email (datos personales sin valor analítico,
-- Ley 1581 de 2012) ni el ingreso exacto (se reemplaza por income_band).
-- La PK es una surrogate key porque en SCD2 un mismo customer_id tiene
-- varias versiones (filas) a lo largo del tiempo.
CREATE TABLE dw.dim_customer (
    customer_key    SERIAL       PRIMARY KEY,
    customer_id     VARCHAR(10)  NOT NULL,          -- natural key (OLTP)
    city            VARCHAR(100),
    segment         VARCHAR(50),                    -- mass, premium, high_value
    income_band     VARCHAR(20),                    -- low, middle, high
    customer_since  DATE,
    -- Control de versiones SCD2: [valid_from, valid_to)
    valid_from      TIMESTAMP    NOT NULL,
    valid_to        TIMESTAMP    NOT NULL DEFAULT '9999-12-31',
    is_current      BOOLEAN      NOT NULL DEFAULT TRUE,
    CONSTRAINT ck_customer_validity CHECK (valid_from < valid_to)
);
-- Una sola versión vigente por cliente
CREATE UNIQUE INDEX ux_dim_customer_current
    ON dw.dim_customer (customer_id) WHERE is_current;
-- Búsqueda de la versión vigente en una fecha (lookup del ELT)
CREATE INDEX idx_dim_customer_nk
    ON dw.dim_customer (customer_id, valid_from, valid_to);


-- ── TODO 2: Dimensión comercio ─────────────────────────────
-- SCD Tipo 1: si cambia el nombre o la categoría se sobrescribe.
CREATE TABLE dw.dim_merchant (
    merchant_key    SERIAL       PRIMARY KEY,
    merchant_id     VARCHAR(10)  NOT NULL UNIQUE,   -- natural key (OLTP)
    merchant_name   VARCHAR(200) NOT NULL,
    category        VARCHAR(100),
    city            VARCHAR(100),
    is_online       BOOLEAN      NOT NULL
);


-- ── TODO 3: Dimensión método de pago ───────────────────────
CREATE TABLE dw.dim_payment_method (
    payment_method_key  SERIAL       PRIMARY KEY,
    method_name         VARCHAR(50)  NOT NULL UNIQUE,   -- natural key (OLTP)
    method_type         VARCHAR(30)  NOT NULL           -- card, digital_wallet, bank_transfer
);


-- ── TODO 4: Tabla de hechos ────────────────────────────────
-- Grano: una fila por transacción.
-- Cada foreign key debe referenciar la PK de su dimensión.
CREATE TABLE dw.fact_transactions (
    transaction_id      VARCHAR(20)   PRIMARY KEY,      -- dimensión degenerada; garantiza el grano
    date_key            INT           NOT NULL REFERENCES dw.dim_date(date_key),
    time_key            INT           NOT NULL REFERENCES dw.dim_time(time_key),
    customer_key        INT           NOT NULL REFERENCES dw.dim_customer(customer_key),
    merchant_key        INT           NOT NULL REFERENCES dw.dim_merchant(merchant_key),
    payment_method_key  INT           NOT NULL REFERENCES dw.dim_payment_method(payment_method_key),
    -- Medidas
    amount_cop          NUMERIC(15,2) NOT NULL,         -- aditiva
    is_fraud            BOOLEAN       NOT NULL,         -- flag: aditiva como SUM(is_fraud::int)
    is_approved         BOOLEAN       NOT NULL,         -- flag: aditiva como SUM(is_approved::int)
    fraud_risk_score    NUMERIC(5,4)                    -- no aditiva (AVG/MAX); NULL si no hubo evento
);


-- ── TODO 5: Índices para las consultas analíticas ──────────
-- Revisa los filtros y JOIN de olap_queries.sql. ¿Qué columnas de la
-- tabla de hechos conviene indexar? Mide el efecto con EXPLAIN ANALYZE.
-- Resultado del experimento con EXPLAIN ANALYZE (ver informe):
--  * idx_fact_date: Q1 y Q3 filtran por rango de fechas a través de dim_date.
--    Como los hechos se cargan en orden cronológico (correlación 1.0), Q3
--    usa Nested Loop + Index Scan y lee ~10 % de las páginas.
--  * idx_fact_customer: no lo usan Q1-Q3 (agregan todo), pero sirve para el
--    drill-down de un cliente (investigación de fraude), muy selectivo.
--  * merchant_key NO se indexa: solo hay 8 comercios (~12,5 % de filas cada
--    uno); el optimizador siempre prefiere Seq Scan + Hash Join.
CREATE INDEX idx_fact_date      ON dw.fact_transactions (date_key);
CREATE INDEX idx_fact_customer  ON dw.fact_transactions (customer_key);
