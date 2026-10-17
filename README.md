# SI7006 · Big Data Labs 2026-2

Laboratorios prácticos para el curso **Almacenamiento y Procesamiento de Grandes Datos** — Maestría en Ingeniería de Datos, EAFIT.

**Caso de uso:** PayTrack Colombia — plataforma de pagos digitales con 500K transacciones diarias, análisis de fraude en tiempo real y conciliación batch.

> La infraestructura en AWS se despliega con Serverless Framework v3.
> Los labs de Delta Lake corren en Databricks Free Edition (gratis).

---

## Laboratorios

| Lab | Tema | Infraestructura | Semana |
|-----|------|-----------------|--------|
| [Lab 1](labs/01-modelado/) | Modelado OLTP → OLAP: de transacciones a esquema estrella | Docker + PostgreSQL | S1 |
| [Lab 2](labs/02-data-lake/) | Data Lake en S3: zonas raw / processed / curated | AWS (free tier) | S2 |
| [Lab 3](labs/03-lakehouse/) | Lakehouse con Delta Lake: ACID, time travel y schema evolution | Databricks Free Edition | S3 |
| [Lab 4](labs/04-medallion/) | Arquitectura Medallion: Bronze → Silver → Gold con Spark | Databricks Free Edition | S4 |
| [Lab 5](labs/05-streaming/) | Streaming de eventos de fraude con Kinesis + Lambda | AWS (de pago, < USD 0.50) | S5 |

## Cómo funcionan los labs

Cada lab es un **reto**: el código mecánico (conexiones, infraestructura, lectura y escritura de archivos) está dado, y las partes que aplican el tema de la semana están marcadas con `TODO` para que las completes tú.

Cada lab incluye **autoevaluación**: tests, scripts de validación o celdas de verificación que te dicen si tu solución es correcta (`✓`) o qué falla (`✗`). No los modifiques: son los mismos criterios con los que se califica la parte técnica. Las verificaciones confirman que tu código funciona; las decisiones de diseño y su justificación las evalúa tu informe.

---

## Requisitos previos

- Docker Desktop ≥ 4.x con WSL 2 (Windows), o Docker Desktop / colima (Mac)
- Python ≥ 3.11 y [`uv`](https://docs.astral.sh/uv/)
- Cuenta de AWS con CLI configurada (`aws configure`) — desde el Lab 2
- Node.js ≥ 20 y Serverless Framework v3 (`npm install -g serverless@3`) — desde el Lab 2
- Cuenta gratuita en [Databricks Free Edition](https://www.databricks.com/learn/free-edition) — desde el Lab 3

---

## Setup rápido

```bash
# 1. Clonar el repositorio
git clone https://github.com/asacrea/big-data-labs-2026-2.git
cd big-data-labs-2026-2

# 2. Instalar dependencias de Python
uv sync

# 3. Levantar PostgreSQL y pgAdmin (Lab 1)
docker compose -f .docker/docker-compose.yml up -d postgres pgadmin

# 4. Desde el Lab 2: variables de entorno de AWS
cp .envs/aws.env.example .envs/aws.env
# Edita .envs/aws.env con tus credenciales y tu STUDENT_ID
```

---

## Caso de uso: PayTrack Colombia

PayTrack es una fintech colombiana con los siguientes flujos de datos:

| Fuente | Volumen | Frecuencia |
|--------|---------|------------|
| Transacciones POS / app | 500K / día | En tiempo real |
| Eventos de fraude | 2K / día | En tiempo real |
| Conciliación bancaria | 1 archivo / banco | Batch nocturno |
| Reportes regulatorios | Mensual | Batch |

Los labs van construyendo la arquitectura completa de esta plataforma sesión a sesión.

---

## Estructura del repositorio

```
.
├── .docker/          # Dockerfile y docker-compose
├── .envs/            # Variables de entorno (no se suben a git)
├── labs/
│   ├── shared/       # Generador de datos sintéticos de PayTrack
│   ├── 01-modelado/  # Lab 1: OLTP → OLAP (PostgreSQL)
│   ├── 02-data-lake/ # Lab 2: Data Lake en S3 + Glue + Athena
│   ├── 03-lakehouse/ # Lab 3: Delta Lake en Databricks
│   ├── 04-medallion/ # Lab 4: Arquitectura Medallion en Databricks
│   └── 05-streaming/ # Lab 5: Kinesis + Lambda
└── pyproject.toml    # Dependencias de Python (uv)
```

---

## Licencia

MIT — Andrés Acrea, EAFIT 2026
