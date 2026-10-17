-- ============================================================
-- Consultas Athena sobre el archivo histórico del stream
-- Ejecútalas en la consola de Athena (workgroup primary o el tuyo).
-- ============================================================

-- ── Dado: tabla sobre el archivo en S3 ──────────────────────
-- Reemplaza <ARCHIVE_BUCKET> por el nombre de tu bucket (salida del deploy).
-- Usa "partition projection": Athena calcula las particiones a partir de la
-- ruta, sin necesidad de crawler ni de MSCK REPAIR TABLE.
CREATE DATABASE IF NOT EXISTS paytrack_stream;

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_stream.fraud_stream_archive (
    transaction_id  STRING,
    customer_id     STRING,
    merchant_id     STRING,
    amount_cop      DOUBLE,
    payment_method  STRING,
    country         STRING,
    `timestamp`     STRING,
    patterns        ARRAY<STRING>,
    risk_score      DOUBLE,
    detected_at     STRING,
    latency_ms      BIGINT
)
PARTITIONED BY (year STRING, month STRING, day STRING, hour STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://<ARCHIVE_BUCKET>/archive/'
TBLPROPERTIES (
    'projection.enabled'      = 'true',
    'projection.year.type'    = 'integer', 'projection.year.range'  = '2026,2030',
    'projection.month.type'   = 'integer', 'projection.month.range' = '1,12', 'projection.month.digits' = '2',
    'projection.day.type'     = 'integer', 'projection.day.range'   = '1,31', 'projection.day.digits'   = '2',
    'projection.hour.type'    = 'integer', 'projection.hour.range'  = '0,23', 'projection.hour.digits'  = '2',
    'storage.location.template' = 's3://<ARCHIVE_BUCKET>/archive/year=${year}/month=${month}/day=${day}/hour=${hour}'
);

-- ── TODO 1 ──────────────────────────────────────────────────
-- Por patrón de fraude: número de alertas y risk score promedio.
-- Pista: patterns es un arreglo; una transacción puede tener varios patrones
-- (busca CROSS JOIN UNNEST en la documentación de Athena).


-- ── TODO 2 ──────────────────────────────────────────────────
-- Por minuto: transacciones procesadas, transacciones con alerta y
-- latencia promedio y máxima (latency_ms). Ordena por minuto.


-- ── TODO 3 ──────────────────────────────────────────────────
-- Los 10 clientes con más transacciones marcadas como rapid_sequence
-- y el monto total de esas transacciones.

