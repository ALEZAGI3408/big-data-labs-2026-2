# Lab 1 · Entrega — Alejandro Zapata

| Entregable | Archivo |
|------------|---------|
| Esquema estrella | [`../sql/star_schema.sql`](../sql/star_schema.sql) |
| Carga ELT | [`../load_star.py`](../load_star.py) |
| Consultas OLAP | [`../sql/olap_queries.sql`](../sql/olap_queries.sql) |
| Captura `load_star.py` (validaciones en verde) | [`captura_load_star.png`](captura_load_star.png) · [texto](salida_load_star.txt) |
| Captura `check_queries.py` (3/3) | [`captura_check_queries.png`](captura_check_queries.png) · [texto](salida_check_queries.txt) |
| Diagrama del esquema estrella (draw.io) | [`diagrama_esquema_estrella.drawio`](diagrama_esquema_estrella.drawio) · [PNG](diagrama_esquema_estrella.png) |
| Informe (PDF, 3 páginas) | [`informe_lab1.pdf`](informe_lab1.pdf) |

Material de apoyo del análisis de rendimiento:

- [`../benchmark.py`](../benchmark.py): mide OLTP vs OLAP y el efecto de cada índice (mediana de 5 ejecuciones de `EXPLAIN (ANALYZE, BUFFERS)`).
- [`../resultados/`](../resultados/): planes de ejecución completos con 50K y 500K transacciones.

En Windows, ejecuta los scripts con `PYTHONUTF8=1` (en PowerShell: `$env:PYTHONUTF8=1`) para que Python lea y escriba los archivos en UTF-8.
