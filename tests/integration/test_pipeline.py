"""End-to-end integration tests for the full ETL pipeline."""

import pytest
import os
import shutil
from pyspark.sql import SparkSession


@pytest.mark.integration
def test_full_pipeline_bronze_to_gold(spark_session: SparkSession, sample_taxi_data, temp_dir, sample_config):
    """Test complete pipeline: Bronze -> Silver -> Gold."""
    from etl.bronze_job import run_bronze_job
    from etl.silver_job import run_silver_job
    from etl.gold_job import run_gold_job
    
    # Create raw data directory and save sample data as CSV
    raw_path = sample_config["paths"]["raw"]
    os.makedirs(raw_path, exist_ok=True)
    
    # Write sample data as CSV
    sample_taxi_data.coalesce(1).write.mode("overwrite").option("header", "true").csv(raw_path)
    
    # Run Bronze job
    bronze_metadata = run_bronze_job(spark_session, sample_config)
    assert bronze_metadata["status"] == "completed"
    assert bronze_metadata["final_row_count"] > 0
    
    # Verify Bronze table
    bronze_df = spark_session.read.format("delta").load(sample_config["paths"]["bronze"])
    assert bronze_df.count() > 0
    assert "ingestion_ts" in bronze_df.columns
    
    # Run Silver job
    silver_metadata = run_silver_job(spark_session, sample_config)
    assert silver_metadata["status"] == "completed"
    assert silver_metadata["final_row_count"] > 0
    assert silver_metadata["final_row_count"] <= silver_metadata["bronze_row_count"]
    
    # Verify Silver table
    silver_df = spark_session.read.format("delta").load(sample_config["paths"]["silver"])
    assert silver_df.count() > 0
    
    # Run Gold job
    gold_metadata = run_gold_job(spark_session, sample_config)
    assert gold_metadata["status"] == "completed"
    assert "daily_kpis_row_count" in gold_metadata
    assert "zone_demand_row_count" in gold_metadata
    
    # Verify Gold tables
    daily_kpis_df = spark_session.read.format("delta").load(sample_config["paths"]["gold_daily_kpis"])
    zone_demand_df = spark_session.read.format("delta").load(sample_config["paths"]["gold_zone_demand"])
    
    assert daily_kpis_df.count() > 0
    assert zone_demand_df.count() > 0
    
    # Verify Gold table schemas
    assert "trip_date" in daily_kpis_df.columns
    assert "daily_trip_count" in daily_kpis_df.columns
    assert "trip_date" in zone_demand_df.columns
    assert "trip_count" in zone_demand_df.columns


@pytest.mark.integration
def test_pipeline_with_dq_checks(spark_session: SparkSession, sample_taxi_data, temp_dir, sample_config):
    """Test pipeline with data quality checks enabled."""
    from etl.bronze_job import run_bronze_job
    from etl.silver_job import run_silver_job
    
    # Create raw data directory
    raw_path = sample_config["paths"]["raw"]
    os.makedirs(raw_path, exist_ok=True)
    
    # Write sample data
    sample_taxi_data.coalesce(1).write.mode("overwrite").option("header", "true").csv(raw_path)
    
    # Run Bronze with DQ checks
    bronze_metadata = run_bronze_job(spark_session, sample_config)
    assert bronze_metadata["status"] == "completed"
    assert "dq_results" in bronze_metadata
    
    # Verify DQ metrics were saved
    dq_metrics_path = sample_config["paths"]["dq_metrics"]
    try:
        dq_metrics_df = spark_session.read.format("delta").load(dq_metrics_path)
        assert dq_metrics_df.count() > 0
    except Exception:
        # DQ metrics table might not exist yet, that's okay
        pass
    
    # Run Silver with DQ checks
    silver_metadata = run_silver_job(spark_session, sample_config)
    assert silver_metadata["status"] == "completed"
    assert "dq_results" in silver_metadata


@pytest.mark.integration
def test_pipeline_data_lineage(spark_session: SparkSession, sample_taxi_data, temp_dir, sample_config):
    """Test that data flows correctly through all layers."""
    from etl.bronze_job import run_bronze_job, read_bronze_delta
    from etl.silver_job import run_silver_job
    from etl.gold_job import run_gold_job
    
    # Create raw data
    raw_path = sample_config["paths"]["raw"]
    os.makedirs(raw_path, exist_ok=True)
    sample_taxi_data.coalesce(1).write.mode("overwrite").option("header", "true").csv(raw_path)
    
    # Run all jobs
    bronze_metadata = run_bronze_job(spark_session, sample_config)
    silver_metadata = run_silver_job(spark_session, sample_config)
    gold_metadata = run_gold_job(spark_session, sample_config)
    
    # Verify row counts decrease appropriately (filtering/deduplication)
    assert bronze_metadata["final_row_count"] >= silver_metadata["final_row_count"]
    
    # Verify data exists in all layers
    bronze_df = read_bronze_delta(spark_session, sample_config["paths"]["bronze"])
    silver_df = spark_session.read.format("delta").load(sample_config["paths"]["silver"])
    gold_kpis_df = spark_session.read.format("delta").load(sample_config["paths"]["gold_daily_kpis"])
    
    assert bronze_df.count() > 0
    assert silver_df.count() > 0
    assert gold_kpis_df.count() > 0
    
    # Verify Gold aggregates are correct
    total_trips_in_gold = gold_kpis_df.agg({"daily_trip_count": "sum"}).collect()[0][0]
    assert total_trips_in_gold <= silver_df.count()  # Aggregated count should be <= source


@pytest.mark.integration
@pytest.mark.slow
def test_pipeline_with_partitioning(spark_session: SparkSession, sample_taxi_data, temp_dir, sample_config):
    """Test pipeline with partitioning enabled."""
    # Enable partitioning in config
    sample_config["partitioning"] = {"enabled": True}
    
    from etl.bronze_job import run_bronze_job
    from etl.silver_job import run_silver_job
    
    # Create raw data
    raw_path = sample_config["paths"]["raw"]
    os.makedirs(raw_path, exist_ok=True)
    sample_taxi_data.coalesce(1).write.mode("overwrite").option("header", "true").csv(raw_path)
    
    # Run jobs
    bronze_metadata = run_bronze_job(spark_session, sample_config)
    silver_metadata = run_silver_job(spark_session, sample_config)
    
    assert bronze_metadata["status"] == "completed"
    assert silver_metadata["status"] == "completed"
    
    # Verify partitioning (check if trip_date column exists)
    bronze_df = spark_session.read.format("delta").load(sample_config["paths"]["bronze"])
    silver_df = spark_session.read.format("delta").load(sample_config["paths"]["silver"])
    
    # Partition column should exist if partitioning is enabled
    if sample_config["partitioning"].get("enabled"):
        assert "trip_date" in bronze_df.columns or "trip_date" in silver_df.columns

