"""
Utilidades compartidas por los jobs del Lab 2 (dadas, no las modifiques).

Los mismos jobs corren en dos lugares:
  - AWS Glue 5.0: BASE = s3://<bucket>
  - Local (Docker + Spark): BASE = /workspace/lakehouse-local
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession


def parse_args(names: list[str]) -> dict:
    """Lee --NOMBRE valor. En Glue usa getResolvedOptions; en local, argparse."""
    try:
        from awsglue.utils import getResolvedOptions

        return getResolvedOptions(sys.argv, names)
    except ImportError:
        import argparse

        parser = argparse.ArgumentParser()
        for name in names:
            parser.add_argument(f"--{name}", required=True)
        return vars(parser.parse_known_args()[0])


def get_spark(app: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "America/Bogota")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def table_path(base: str, layer: str, name: str) -> str:
    """Ruta de una tabla Delta: <base>/lakehouse/<capa>/<tabla>"""
    return f"{base}/lakehouse/{layer}/{name}"


def table_exists(spark: SparkSession, path: str) -> bool:
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, path)


def read_table(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def check(condition: bool, message: str) -> None:
    """Autoevaluación: si falla, el job termina con error y muestra el motivo."""
    if not condition:
        raise AssertionError(f"✗ {message}")
    print(f"✓ {message}")
