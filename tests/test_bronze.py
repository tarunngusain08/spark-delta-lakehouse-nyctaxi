"""Unit tests for Bronze layer ETL job."""

import pytest
from pyspark.sql import SparkSession
from etl.bronze_job import (
    read_raw_csv, add_metadata_columns, write_bronze_delta,
    read_bronze_delta, run_bronze_job
)
from utils.schemas import get_bronze_schema, validate_schema
import os


@pytest.mark.unit
def test_add_metadata_columns(spark_session: SparkSession, sample_taxi_data):
    """Test that metadata columns are added correctly."""
    df_with_metadata = add_metadata_columns(sample_taxi_data)
    
    assert "ingestion_ts" in df_with_metadata.columns
    assert "source_file" in df_with_metadata.columns
    assert df_with_metadata.count() == sample_taxi_data.count()


@pytest.mark.unit
def test_bronze_schema_validation(spark_session: SparkSession, sample_taxi_data):
    """Test Bronze schema validation."""
    df_with_metadata = add_metadata_columns(sample_taxi_data)
    bronze_schema = get_bronze_schema()
    
    is_valid, errors = validate_schema(
        df_with_metadata.schema,
        bronze_schema,
        allow_extra_columns=True
    )
    
    # Should be valid (allowing extra columns)
    assert is_valid or len(errors) == 0  # May have minor type mismatches


@pytest.mark.integration
def test_bronze_write_read(spark_session: SparkSession, sample_taxi_data, temp_dir, sample_config):
    """Test writing and reading Bronze Delta table."""
    bronze_path = sample_config["paths"]["bronze"]
    
    # Add metadata and write
    df_with_metadata = add_metadata_columns(sample_taxi_data)
    write_bronze_delta(df_with_metadata, bronze_path, mode="overwrite")
    
    # Read back
    bronze_readback = read_bronze_delta(spark_session, bronze_path)
    
    assert bronze_readback.count() == sample_taxi_data.count()
    assert "ingestion_ts" in bronze_readback.columns
    assert "source_file" in bronze_readback.columns


@pytest.mark.integration
def test_bronze_job_integration(spark_session: SparkSession, sample_taxi_data, temp_dir, sample_config):
    """Test full Bronze job with sample data."""
    import shutil
    
    # Create raw data directory and save sample data as CSV
    raw_path = sample_config["paths"]["raw"]
    os.makedirs(raw_path, exist_ok=True)
    
    # Write sample data as CSV
    sample_taxi_data.coalesce(1).write.mode("overwrite").option("header", "true").csv(raw_path)
    
    # Run Bronze job
    metadata = run_bronze_job(spark_session, sample_config)
    
    assert metadata["status"] == "completed"
    assert metadata["final_row_count"] == sample_taxi_data.count()
    assert "dq_results" in metadata
    
    # Verify Bronze table exists
    bronze_readback = read_bronze_delta(spark_session, sample_config["paths"]["bronze"])
    assert bronze_readback.count() > 0

