"""Silver layer ETL job - Data cleaning, typing, and deduplication."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, trim, lower, to_date
)
from pyspark.sql.types import DoubleType, IntegerType, TimestampType
from typing import Dict, List, Optional
import logging

from utils.schemas import get_silver_schema, validate_schema, enforce_schema
from utils.data_quality import DataQualityFramework, create_default_dq_framework
from etl.dq_metrics import create_dq_metrics_table, save_dq_results, generate_run_id
from utils.delta_ops import optimize_table

logger = logging.getLogger("nyc_taxi_etl")


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


def cast_columns(df: DataFrame) -> DataFrame:
    """
    Cast columns to proper data types.
    
    Args:
        df: Input DataFrame
    
    Returns:
        DataFrame with properly typed columns
    """
    logger.info("Casting columns to proper data types")
    
    # Common column mappings (handle case-insensitive column names)
    df_typed = df
    
    # Timestamp columns
    timestamp_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    for col_name in timestamp_cols:
        if col_name.lower() in [c.lower() for c in df.columns]:
            actual_col = [c for c in df.columns if c.lower() == col_name.lower()][0]
            df_typed = df_typed.withColumn(
                col_name.lower(),
                to_timestamp(col(actual_col))
            )
    
    # Numeric columns
    numeric_mappings = {
        "passenger_count": IntegerType(),
        "trip_distance": DoubleType(),
        "pulocationid": IntegerType(),
        "dolocationid": IntegerType(),
        "fare_amount": DoubleType(),
        "extra": DoubleType(),
        "mta_tax": DoubleType(),
        "tip_amount": DoubleType(),
        "tolls_amount": DoubleType(),
        "total_amount": DoubleType(),
        "payment_type": IntegerType(),
        "vendorid": IntegerType(),
        "ratecodeid": IntegerType(),
    }
    
    for col_name, dtype in numeric_mappings.items():
        # Find case-insensitive match
        matching_cols = [c for c in df_typed.columns if c.lower() == col_name.lower()]
        if matching_cols:
            actual_col = matching_cols[0]
            try:
                df_typed = df_typed.withColumn(
                    col_name.lower(),
                    col(actual_col).cast(dtype)
                )
            except Exception as e:
                logger.warning(f"Could not cast {actual_col} to {dtype}: {e}")
    
    # String columns - standardize to lowercase
    string_cols = ["store_and_fwd_flag"]
    for col_name in string_cols:
        matching_cols = [c for c in df_typed.columns if c.lower() == col_name.lower()]
        if matching_cols:
            actual_col = matching_cols[0]
            df_typed = df_typed.withColumn(
                col_name.lower(),
                lower(trim(col(actual_col)))
            )
    
    # Standardize all column names to lowercase with underscores
    for old_col in df_typed.columns:
        new_col = old_col.lower().replace(" ", "_")
        if old_col != new_col:
            df_typed = df_typed.withColumnRenamed(old_col, new_col)
    
    return df_typed


def apply_data_quality_filters(df: DataFrame, config: Dict) -> DataFrame:
    """
    Apply data quality filters to remove invalid records.
    
    Args:
        df: Input DataFrame
        config: Configuration dictionary with data quality thresholds
    
    Returns:
        Filtered DataFrame
    """
    logger.info("Applying data quality filters")
    
    initial_count = df.count()
    logger.info(f"Row count before filters: {initial_count}")
    
    dq_config = config.get("data_quality", {})
    
    # Build filter conditions
    filters = []
    
    # Trip distance must be positive
    if "trip_distance" in [c.lower() for c in df.columns]:
        min_distance = dq_config.get("min_trip_distance", 0.0)
        filters.append(col("trip_distance") > min_distance)
    
    # Fare amount must be non-negative
    if "fare_amount" in [c.lower() for c in df.columns]:
        min_fare = dq_config.get("min_fare_amount", 0.0)
        filters.append(col("fare_amount") >= min_fare)
    
    # Total amount must be non-negative
    if "total_amount" in [c.lower() for c in df.columns]:
        min_total = dq_config.get("min_total_amount", 0.0)
        filters.append(col("total_amount") >= min_total)
    
    # Critical columns must not be null
    critical_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    for col_name in critical_cols:
        if col_name.lower() in [c.lower() for c in df.columns]:
            filters.append(col(col_name.lower()).isNotNull())
    
    # Apply all filters
    if filters:
        from functools import reduce
        from operator import and_
        combined_filter = reduce(and_, filters)
        filtered_df = df.filter(combined_filter)
    else:
        filtered_df = df
    
    final_count = filtered_df.count()
    logger.info(f"Row count after filters: {final_count}")
    logger.info(f"Rows filtered out: {initial_count - final_count}")
    
    return filtered_df


def deduplicate_data(df: DataFrame, dedup_columns: List[str]) -> DataFrame:
    """
    Remove duplicate records based on specified columns.
    
    Args:
        df: Input DataFrame
        dedup_columns: List of column names to use for deduplication
    
    Returns:
        Deduplicated DataFrame
    """
    logger.info(f"Deduplicating data using columns: {dedup_columns}")
    
    initial_count = df.count()
    
    # Filter to only columns that exist in the DataFrame
    existing_dedup_cols = [
        col_name for col_name in dedup_columns
        if col_name.lower() in [c.lower() for c in df.columns]
    ]
    
    if not existing_dedup_cols:
        logger.warning("No deduplication columns found in DataFrame. Skipping deduplication.")
        return df
    
    # Get actual column names (case-insensitive match)
    actual_cols = []
    for dedup_col in existing_dedup_cols:
        matching = [c for c in df.columns if c.lower() == dedup_col.lower()]
        if matching:
            actual_cols.append(matching[0])
    
    logger.info(f"Using columns for deduplication: {actual_cols}")
    
    deduplicated_df = df.dropDuplicates(subset=actual_cols)
    
    final_count = deduplicated_df.count()
    logger.info(f"Row count before deduplication: {initial_count}")
    logger.info(f"Row count after deduplication: {final_count}")
    logger.info(f"Duplicate rows removed: {initial_count - final_count}")
    
    return deduplicated_df


def write_silver_delta(
    df: DataFrame,
    silver_path: str,
    mode: str = "overwrite",
    partition_by: Optional[str] = None
) -> None:
    """
    Write DataFrame to Silver Delta table.
    
    Args:
        df: DataFrame to write
        silver_path: Path to Silver Delta table
        mode: Write mode ("overwrite" or "append")
        partition_by: Optional column name to partition by
    """
    logger.info(f"Writing Silver Delta table to: {silver_path} (mode: {mode})")
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
    
    writer.save(silver_path)
    
    logger.info(f"Silver Delta table written successfully")


def run_silver_job(spark: SparkSession, config: Dict, run_id: Optional[str] = None) -> Dict:
    """
    Execute Silver layer ETL job.
    
    This job:
    1. Reads from Bronze Delta table
    2. Casts columns to proper types
    3. Applies data quality filters
    4. Deduplicates records
    5. Validates schema
    6. Runs data quality checks
    7. Writes to Silver Delta table
    
    Args:
        spark: SparkSession instance
        config: Configuration dictionary with paths and settings
        run_id: Optional run ID for tracking. If None, generates one.
    
    Returns:
        Dictionary with job execution metadata
    """
    logger.info("=" * 60)
    logger.info("Starting Silver Layer ETL Job")
    logger.info("=" * 60)
    
    if run_id is None:
        run_id = generate_run_id()
    
    job_metadata = {
        "run_id": run_id,
        "layer": "silver",
        "status": "running"
    }
    
    try:
        # Read Bronze Delta table
        bronze_df = read_bronze_delta(spark, config["paths"]["bronze"])
        bronze_row_count = bronze_df.count()
        job_metadata["bronze_row_count"] = bronze_row_count
        
        # Cast columns to proper types
        typed_df = cast_columns(bronze_df)
        
        # Apply data quality filters
        filtered_df = apply_data_quality_filters(typed_df, config)
        filtered_row_count = filtered_df.count()
        job_metadata["filtered_row_count"] = filtered_row_count
        
        # Deduplicate
        dedup_config = config.get("deduplication", {})
        dedup_columns = dedup_config.get("dedup_columns", [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "vendorid",
            "total_amount"
        ])
        silver_df = deduplicate_data(filtered_df, dedup_columns)
        dedup_row_count = silver_df.count()
        job_metadata["dedup_row_count"] = dedup_row_count
        
        # Schema validation and enforcement
        dq_config = config.get("data_quality", {})
        if dq_config.get("enable_schema_validation", True):
            logger.info("Validating Silver schema")
            silver_schema = get_silver_schema()
            silver_df_enforced, warnings = enforce_schema(silver_df, silver_schema, "silver")
            
            for warning in warnings:
                logger.warning(f"Schema enforcement warning: {warning}")
            
            # Validate schema (stricter for Silver)
            is_valid, errors = validate_schema(
                silver_df_enforced.schema,
                silver_schema,
                allow_extra_columns=False  # Silver should match schema exactly
            )
            
            if errors:
                logger.warning(f"Schema validation found {len(errors)} issues:")
                for error in errors[:10]:
                    logger.warning(f"  - {error}")
                if dq_config.get("fail_on_dq_errors", True):
                    raise ValueError(f"Silver schema validation failed: {errors[0]}")
            
            silver_df = silver_df_enforced
        
        # Data Quality checks
        dq_framework = create_default_dq_framework(spark, config)
        dq_results = dq_framework.run_all_checks(silver_df, layer_name="silver")
        job_metadata["dq_results"] = dq_results
        
        # Check if we should fail on DQ errors
        fail_on_errors = dq_config.get("fail_on_dq_errors", True)
        failed_checks = [r for r in dq_results if not r.get("passed", False) and r.get("severity") == "ERROR"]
        
        if failed_checks and fail_on_errors:
            error_msg = f"Silver job failed due to {len(failed_checks)} DQ errors"
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
        
        # Write to Silver Delta table
        write_silver_delta(silver_df, config["paths"]["silver"], mode="overwrite")
        
        # Verify by reading back
        silver_readback = spark.read.format("delta").load(config["paths"]["silver"])
        final_row_count = silver_readback.count()
        job_metadata["final_row_count"] = final_row_count
        job_metadata["status"] = "completed"
        
        logger.info(f"Silver job completed successfully. Final row count: {final_row_count}")
        
        logger.info("=" * 60)
        logger.info("Silver Layer ETL Job Completed")
        logger.info("=" * 60)
        
        return job_metadata
        
    except Exception as e:
        logger.error(f"Silver job failed with error: {str(e)}", exc_info=True)
        job_metadata["status"] = "failed"
        job_metadata["error"] = str(e)
        raise

