"""Bronze layer ETL job - Raw data ingestion."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name, col, to_date
from typing import Dict, Optional
import logging
from datetime import datetime

from utils.schemas import get_bronze_schema, validate_schema, enforce_schema
from utils.data_quality import DataQualityFramework, create_default_dq_framework
from etl.dq_metrics import create_dq_metrics_table, save_dq_results, generate_run_id
from utils.delta_ops import optimize_table

logger = logging.getLogger("nyc_taxi_etl")


def read_raw_csv(spark: SparkSession, raw_path: str) -> DataFrame:
    """
    Read raw CSV files from the specified path.
    
    Args:
        spark: SparkSession instance
        raw_path: Path to raw CSV files (can be directory or file)
    
    Returns:
        DataFrame with raw CSV data
    """
    logger.info(f"Reading raw CSV data from: {raw_path}")
    
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(raw_path)
    )
    
    logger.info(f"Raw data schema inferred. Row count: {df.count()}")
    return df


def add_metadata_columns(df: DataFrame) -> DataFrame:
    """
    Add metadata columns to track ingestion.
    
    Args:
        df: Input DataFrame
    
    Returns:
        DataFrame with added metadata columns
    """
    df_with_metadata = (
        df
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_file", input_file_name())
    )
    
    return df_with_metadata


def write_bronze_delta(
    df: DataFrame,
    bronze_path: str,
    mode: str = "overwrite",
    partition_by: Optional[str] = None
) -> None:
    """
    Write DataFrame to Bronze Delta table.
    
    Args:
        df: DataFrame to write
        bronze_path: Path to Bronze Delta table
        mode: Write mode ("overwrite" or "append")
        partition_by: Optional column name to partition by
    """
    logger.info(f"Writing Bronze Delta table to: {bronze_path} (mode: {mode})")
    if partition_by:
        logger.info(f"Partitioning by: {partition_by}")
    
    writer = (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
    )
    
    if partition_by and partition_by in df.columns:
        writer = writer.partitionBy(partition_by)
    
    writer.save(bronze_path)
    
    logger.info(f"Bronze Delta table written successfully")


def read_bronze_delta(spark: SparkSession, bronze_path: str) -> DataFrame:
    """
    Read Bronze Delta table.
    
    Args:
        spark: SparkSession instance
        bronze_path: Path to Bronze Delta table
    
    Returns:
        DataFrame from Bronze Delta table
    """
    logger.info(f"Reading Bronze Delta table from: {bronze_path}")
    
    df = spark.read.format("delta").load(bronze_path)
    logger.info(f"Bronze table row count: {df.count()}")
    
    return df


def run_bronze_job(spark: SparkSession, config: Dict, run_id: Optional[str] = None) -> Dict:
    """
    Execute Bronze layer ETL job.
    
    This job:
    1. Reads raw CSV files
    2. Adds metadata columns (ingestion_ts, source_file)
    3. Validates schema (if enabled)
    4. Runs data quality checks
    5. Writes to Bronze Delta table
    
    Args:
        spark: SparkSession instance
        config: Configuration dictionary with paths and settings
        run_id: Optional run ID for tracking. If None, generates one.
    
    Returns:
        Dictionary with job execution metadata (row counts, DQ results, etc.)
    """
    logger.info("=" * 60)
    logger.info("Starting Bronze Layer ETL Job")
    logger.info("=" * 60)
    
    if run_id is None:
        run_id = generate_run_id()
    
    job_metadata = {
        "run_id": run_id,
        "layer": "bronze",
        "status": "running"
    }
    
    try:
        # Read raw CSV
        raw_df = read_raw_csv(spark, config["paths"]["raw"])
        initial_row_count = raw_df.count()
        job_metadata["initial_row_count"] = initial_row_count
        
        # Add metadata columns
        bronze_df = add_metadata_columns(raw_df)
        
        # Add partitioning column if enabled
        partition_config = config.get("partitioning", {})
        if partition_config.get("enabled", False):
            partition_col = partition_config.get("bronze_partition_column", "trip_date")
            # Derive trip_date from pickup datetime
            pickup_col = None
            for col_name in bronze_df.columns:
                if "pickup" in col_name.lower() and "datetime" in col_name.lower():
                    pickup_col = col_name
                    break
            
            if pickup_col:
                bronze_df = bronze_df.withColumn(
                    partition_col,
                    to_date(col(pickup_col))
                )
                logger.info(f"Added partition column: {partition_col}")
        
        # Schema validation (if enabled)
        dq_config = config.get("data_quality", {})
        if dq_config.get("enable_schema_validation", True):
            logger.info("Validating Bronze schema")
            bronze_schema = get_bronze_schema()
            bronze_df_enforced, warnings = enforce_schema(bronze_df, bronze_schema, "bronze")
            
            for warning in warnings:
                logger.warning(f"Schema enforcement warning: {warning}")
            
            # Validate schema
            is_valid, errors = validate_schema(
                bronze_df_enforced.schema,
                bronze_schema,
                allow_extra_columns=True  # Allow extra columns in Bronze (raw data)
            )
            
            if errors:
                logger.warning(f"Schema validation found {len(errors)} issues:")
                for error in errors[:10]:  # Log first 10 errors
                    logger.warning(f"  - {error}")
            
            bronze_df = bronze_df_enforced
        
        # Data Quality checks
        dq_framework = create_default_dq_framework(spark, config)
        dq_results = dq_framework.run_all_checks(bronze_df, layer_name="bronze")
        job_metadata["dq_results"] = dq_results
        
        # Check if we should fail on DQ errors
        fail_on_errors = dq_config.get("fail_on_dq_errors", True)
        failed_checks = [r for r in dq_results if not r.get("passed", False) and r.get("severity") == "ERROR"]
        
        if failed_checks and fail_on_errors:
            error_msg = f"Bronze job failed due to {len(failed_checks)} DQ errors"
            logger.error(error_msg)
            job_metadata["status"] = "failed"
            job_metadata["error"] = error_msg
            raise ValueError(error_msg)
        
        # Save DQ metrics
        metrics_path = config["paths"].get("dq_metrics")
        if metrics_path:
            try:
                create_dq_metrics_table(spark, metrics_path)
                save_dq_results(spark, dq_results, run_id, metrics_path)
            except Exception as e:
                logger.warning(f"Failed to save DQ metrics: {str(e)}")
        
        # Determine write mode (incremental or overwrite)
        incremental_config = config.get("incremental", {})
        write_mode = "append" if incremental_config.get("enabled", False) else "overwrite"
        
        # Get partition column if partitioning enabled
        partition_col = None
        partition_config = config.get("partitioning", {})
        if partition_config.get("enabled", False):
            partition_col = partition_config.get("bronze_partition_column", "trip_date")
            if partition_col not in bronze_df.columns:
                partition_col = None
        
        # Write to Bronze Delta table
        write_bronze_delta(
            bronze_df,
            config["paths"]["bronze"],
            mode=write_mode,
            partition_by=partition_col
        )
        
        # Optimize table if enabled
        optimization_config = config.get("delta_optimization", {})
        if optimization_config.get("optimize_after_write", False):
            z_order_cols = optimization_config.get("bronze_z_order_columns")
            optimize_table(spark, config["paths"]["bronze"], z_order_columns=z_order_cols)
        
        # Verify by reading back
        bronze_readback = read_bronze_delta(spark, config["paths"]["bronze"])
        final_row_count = bronze_readback.count()
        job_metadata["final_row_count"] = final_row_count
        job_metadata["status"] = "completed"
        
        logger.info(f"Bronze job completed successfully. Final row count: {final_row_count}")
        
        logger.info("=" * 60)
        logger.info("Bronze Layer ETL Job Completed")
        logger.info("=" * 60)
        
        return job_metadata
        
    except Exception as e:
        logger.error(f"Bronze job failed with error: {str(e)}", exc_info=True)
        job_metadata["status"] = "failed"
        job_metadata["error"] = str(e)
        raise

