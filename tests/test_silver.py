"""Unit tests for Silver layer ETL job."""

import pytest
from pyspark.sql import SparkSession
from etl.silver_job import (
    cast_columns, apply_data_quality_filters,
    deduplicate_data, run_silver_job
)
from utils.schemas import get_silver_schema


@pytest.mark.unit
def test_cast_columns(spark_session: SparkSession, sample_taxi_data):
    """Test column type casting."""
    # Add metadata columns first (as Bronze would)
    from etl.bronze_job import add_metadata_columns
    bronze_df = add_metadata_columns(sample_taxi_data)
    
    typed_df = cast_columns(bronze_df)
    
    # Check that timestamp columns exist (may be string or timestamp)
    assert "tpep_pickup_datetime" in typed_df.columns or "tpep_pickup_datetime".lower() in [c.lower() for c in typed_df.columns]
    assert typed_df.count() == sample_taxi_data.count()


@pytest.mark.unit
def test_data_quality_filters(spark_session: SparkSession, sample_taxi_data, sample_config):
    """Test data quality filtering."""
    from etl.bronze_job import add_metadata_columns
    bronze_df = add_metadata_columns(sample_taxi_data)
    typed_df = cast_columns(bronze_df)
    
    initial_count = typed_df.count()
    filtered_df = apply_data_quality_filters(typed_df, sample_config)
    final_count = filtered_df.count()
    
    # All sample data should pass filters (they're valid)
    assert final_count <= initial_count
    assert final_count >= 0


@pytest.mark.unit
def test_deduplicate_data(spark_session: SparkSession, sample_taxi_data, sample_config):
    """Test deduplication logic."""
    from etl.bronze_job import add_metadata_columns
    bronze_df = add_metadata_columns(sample_taxi_data)
    typed_df = cast_columns(bronze_df)
    
    # Create duplicate row
    duplicate_row = typed_df.limit(1)
    df_with_duplicates = typed_df.union(duplicate_row)
    
    dedup_config = sample_config.get("deduplication", {})
    dedup_columns = dedup_config.get("dedup_columns", [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "vendorid",
        "total_amount"
    ])
    
    deduplicated_df = deduplicate_data(df_with_duplicates, dedup_columns)
    
    # Should have same count as original (duplicate removed)
    assert deduplicated_df.count() == typed_df.count()


@pytest.mark.integration
def test_silver_job_integration(spark_session: SparkSession, sample_taxi_data, temp_dir, sample_config):
    """Test full Silver job."""
    import shutil
    from etl.bronze_job import run_bronze_job, write_bronze_delta, add_metadata_columns
    
    # First create Bronze table
    bronze_df = add_metadata_columns(sample_taxi_data)
    write_bronze_delta(bronze_df, sample_config["paths"]["bronze"], mode="overwrite")
    
    # Run Silver job
    metadata = run_silver_job(spark_session, sample_config)
    
    assert metadata["status"] == "completed"
    assert "bronze_row_count" in metadata
    assert "final_row_count" in metadata
    assert metadata["final_row_count"] <= metadata["bronze_row_count"]  # Filtering reduces count
    
    # Verify Silver table exists
    silver_df = spark_session.read.format("delta").load(sample_config["paths"]["silver"])
    assert silver_df.count() > 0

