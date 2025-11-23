"""Spark session utilities for Delta Lake setup."""

from pyspark.sql import SparkSession
from typing import Optional


def get_spark(app_name: str, master: str = "local[*]") -> SparkSession:
    """
    Create and configure a Spark session with Delta Lake extensions.
    
    Args:
        app_name: Name of the Spark application
        master: Spark master URL (default: "local[*]")
    
    Returns:
        Configured SparkSession with Delta Lake support
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    
    # Set log level to WARN to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def stop_spark(spark: SparkSession) -> None:
    """
    Stop the Spark session.
    
    Args:
        spark: SparkSession to stop
    """
    if spark:
        spark.stop()

