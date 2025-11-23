"""Pytest configuration and fixtures."""

import pytest
from pyspark.sql import SparkSession
from typing import Generator
import tempfile
import shutil
from pathlib import Path


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    """
    Create a Spark session for testing.
    
    Yields:
        SparkSession configured for testing
    """
    spark = (
        SparkSession.builder
        .appName("test_nyc_taxi_etl")
        .master("local[1]")  # Use single core for tests
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.adaptive.enabled", "false")  # Disable for faster tests
        .config("spark.sql.shuffle.partitions", "1")  # Reduce partitions for tests
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("ERROR")  # Reduce log noise
    
    yield spark
    
    spark.stop()


@pytest.fixture(scope="function")
def temp_dir() -> Generator[str, None, None]:
    """
    Create a temporary directory for test data.
    
    Yields:
        Path to temporary directory
    """
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def sample_config(temp_dir: str) -> dict:
    """
    Create a sample configuration dictionary for testing.
    
    Args:
        temp_dir: Temporary directory path
    
    Returns:
        Configuration dictionary
    """
    return {
        "paths": {
            "raw": f"{temp_dir}/raw",
            "bronze": f"{temp_dir}/bronze",
            "silver": f"{temp_dir}/silver",
            "gold_daily_kpis": f"{temp_dir}/gold_daily_kpis",
            "gold_zone_demand": f"{temp_dir}/gold_zone_demand",
            "dq_metrics": f"{temp_dir}/dq_metrics"
        },
        "spark": {
            "app_name": "test_app",
            "master": "local[1]"
        },
        "data_quality": {
            "min_trip_distance": 0.0,
            "min_fare_amount": 0.0,
            "min_total_amount": 0.0,
            "critical_columns": [
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime"
            ],
            "enable_schema_validation": True,
            "fail_on_dq_errors": False  # Don't fail tests on DQ errors
        },
        "deduplication": {
            "dedup_columns": [
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "vendorid",
                "total_amount"
            ]
        },
        "environment": "test"
    }


@pytest.fixture
def sample_taxi_data(spark_session: SparkSession):
    """
    Create sample NYC taxi trip data for testing.
    
    Args:
        spark_session: Spark session
    
    Returns:
        DataFrame with sample taxi trip data
    """
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from datetime import datetime
    
    data = [
        (
            1,  # VendorID
            "2023-01-01 10:00:00",  # tpep_pickup_datetime
            "2023-01-01 10:15:00",  # tpep_dropoff_datetime
            2,  # passenger_count
            2.5,  # trip_distance
            1,  # RatecodeID
            "N",  # store_and_fwd_flag
            1,  # PULocationID
            2,  # DOLocationID
            1,  # payment_type
            10.0,  # fare_amount
            0.5,  # extra
            0.5,  # mta_tax
            2.0,  # tip_amount
            0.0,  # tolls_amount
            0.0,  # improvement_surcharge
            13.0,  # total_amount
            0.0,  # congestion_surcharge
            0.0,  # airport_fee
        ),
        (
            2,
            "2023-01-01 11:00:00",
            "2023-01-01 11:20:00",
            1,
            3.0,
            1,
            "N",
            2,
            3,
            1,
            12.0,
            0.5,
            0.5,
            2.5,
            0.0,
            0.0,
            15.5,
            0.0,
            0.0,
        ),
        (
            1,
            "2023-01-01 12:00:00",
            "2023-01-01 12:10:00",
            1,
            1.0,
            1,
            "N",
            3,
            4,
            2,
            8.0,
            0.5,
            0.5,
            0.0,
            0.0,
            0.0,
            9.0,
            0.0,
            0.0,
        ),
    ]
    
    schema = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", StringType(), True),
        StructField("tpep_dropoff_datetime", StringType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
    ])
    
    return spark_session.createDataFrame(data, schema=schema)

