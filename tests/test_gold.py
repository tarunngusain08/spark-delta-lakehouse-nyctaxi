"""Unit tests for Gold layer ETL job."""

import pytest
from pyspark.sql import SparkSession
from etl.gold_job import (
    create_daily_kpis_table, create_zone_demand_table,
    run_gold_job
)
from utils.schemas import get_gold_daily_kpis_schema, get_gold_zone_demand_schema


@pytest.mark.unit
def test_create_daily_kpis_table(spark_session: SparkSession, sample_taxi_data):
    """Test Daily KPIs table creation."""
    # Create Silver-like DataFrame
    from etl.bronze_job import add_metadata_columns
    from etl.silver_job import cast_columns
    from pyspark.sql.functions import to_timestamp
    
    bronze_df = add_metadata_columns(sample_taxi_data)
    silver_df = cast_columns(bronze_df)
    
    # Ensure timestamp columns are properly cast
    if "tpep_pickup_datetime" in silver_df.columns:
        silver_df = silver_df.withColumn(
            "tpep_pickup_datetime",
            to_timestamp(silver_df["tpep_pickup_datetime"])
        )
    
    daily_kpis_df = create_daily_kpis_table(silver_df)
    
    assert "trip_date" in daily_kpis_df.columns
    assert "daily_trip_count" in daily_kpis_df.columns
    assert "daily_total_revenue" in daily_kpis_df.columns
    assert "avg_trip_distance" in daily_kpis_df.columns
    assert daily_kpis_df.count() > 0


@pytest.mark.unit
def test_create_zone_demand_table(spark_session: SparkSession, sample_taxi_data):
    """Test Zone Demand table creation."""
    from etl.bronze_job import add_metadata_columns
    from etl.silver_job import cast_columns
    from pyspark.sql.functions import to_timestamp
    
    bronze_df = add_metadata_columns(sample_taxi_data)
    silver_df = cast_columns(bronze_df)
    
    # Ensure timestamp columns are properly cast
    if "tpep_pickup_datetime" in silver_df.columns:
        silver_df = silver_df.withColumn(
            "tpep_pickup_datetime",
            to_timestamp(silver_df["tpep_pickup_datetime"])
        )
    
    zone_demand_df = create_zone_demand_table(silver_df)
    
    assert "trip_date" in zone_demand_df.columns
    assert "pu_location_id" in zone_demand_df.columns
    assert "trip_count" in zone_demand_df.columns
    assert "total_revenue" in zone_demand_df.columns
    assert zone_demand_df.count() > 0


@pytest.mark.integration
def test_gold_job_integration(spark_session: SparkSession, sample_taxi_data, temp_dir, sample_config):
    """Test full Gold job."""
    from etl.bronze_job import add_metadata_columns, write_bronze_delta
    from etl.silver_job import run_silver_job
    
    # Create Bronze and Silver tables first
    bronze_df = add_metadata_columns(sample_taxi_data)
    write_bronze_delta(bronze_df, sample_config["paths"]["bronze"], mode="overwrite")
    
    silver_metadata = run_silver_job(spark_session, sample_config)
    assert silver_metadata["status"] == "completed"
    
    # Run Gold job
    gold_metadata = run_gold_job(spark_session, sample_config)
    
    assert gold_metadata["status"] == "completed"
    assert "daily_kpis_row_count" in gold_metadata
    assert "zone_demand_row_count" in gold_metadata
    
    # Verify Gold tables exist
    daily_kpis_df = spark_session.read.format("delta").load(sample_config["paths"]["gold_daily_kpis"])
    zone_demand_df = spark_session.read.format("delta").load(sample_config["paths"]["gold_zone_demand"])
    
    assert daily_kpis_df.count() > 0
    assert zone_demand_df.count() > 0

