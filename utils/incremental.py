"""Incremental processing utilities for CDC and append mode."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, max as spark_max, lit
from delta.tables import DeltaTable
from typing import Dict, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger("nyc_taxi_etl")


def get_watermark(
    spark: SparkSession,
    table_path: str,
    watermark_column: str
) -> Optional[datetime]:
    """
    Get the current watermark (max value) from a Delta table.
    
    Args:
        spark: SparkSession instance
        table_path: Path to Delta table
        watermark_column: Column name to use as watermark
    
    Returns:
        Maximum timestamp value or None if table is empty
    """
    try:
        df = spark.read.format("delta").load(table_path)
        
        if df.count() == 0:
            logger.info(f"Table {table_path} is empty, no watermark found")
            return None
        
        if watermark_column not in df.columns:
            logger.warning(f"Watermark column {watermark_column} not found in table")
            return None
        
        max_value = df.agg(spark_max(col(watermark_column))).collect()[0][0]
        
        if max_value:
            logger.info(f"Watermark for {table_path}: {max_value}")
            return max_value
        else:
            return None
            
    except Exception as e:
        logger.warning(f"Failed to get watermark from {table_path}: {str(e)}")
        return None


def filter_incremental_data(
    df: DataFrame,
    watermark_column: str,
    watermark_value: Optional[datetime],
    initial_load_date: Optional[str] = None
) -> DataFrame:
    """
    Filter DataFrame to include only new data based on watermark.
    
    Args:
        df: Input DataFrame
        watermark_column: Column name to use as watermark
        watermark_value: Current watermark value (max timestamp from target table)
        initial_load_date: Optional initial load date (for first run)
    
    Returns:
        Filtered DataFrame with only new records
    """
    if watermark_column not in df.columns:
        logger.warning(f"Watermark column {watermark_column} not found, returning all data")
        return df
    
    if watermark_value is None:
        # First run - use initial_load_date if provided
        if initial_load_date:
            logger.info(f"First run: filtering data from {initial_load_date}")
            return df.filter(col(watermark_column) >= lit(initial_load_date))
        else:
            logger.info("First run: processing all data")
            return df
    else:
        # Incremental run - only process data newer than watermark
        logger.info(f"Incremental run: filtering data newer than {watermark_value}")
        return df.filter(col(watermark_column) > lit(watermark_value))


def merge_incremental_data(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_keys: list,
    update_columns: Optional[list] = None,
    partition_column: Optional[str] = None
) -> Dict:
    """
    Merge incremental data into target Delta table using MERGE operation.
    
    Args:
        spark: SparkSession instance
        source_df: Source DataFrame with new/updated records
        target_path: Path to target Delta table
        merge_keys: List of column names to use for matching records
        update_columns: Optional list of columns to update (if None, updates all non-key columns)
        partition_column: Optional partition column for optimization
    
    Returns:
        Dictionary with merge results
    """
    try:
        logger.info(f"Merging incremental data into {target_path}")
        logger.info(f"Merge keys: {merge_keys}")
        logger.info(f"Source row count: {source_df.count()}")
        
        delta_table = DeltaTable.forPath(spark, target_path)
        
        # Build merge condition
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
        
        # Determine update columns
        if update_columns is None:
            # Update all columns except merge keys
            update_columns = [col for col in source_df.columns if col not in merge_keys]
        
        # Build update expressions
        update_dict = {col: f"source.{col}" for col in update_columns}
        
        # Perform merge
        (
            delta_table.alias("target")
            .merge(source_df.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_dict)
            .whenNotMatchedInsertAll()
            .execute()
        )
        
        # Get final count
        final_count = spark.read.format("delta").load(target_path).count()
        
        logger.info(f"Merge completed. Final row count: {final_count}")
        
        return {
            "target_path": target_path,
            "source_row_count": source_df.count(),
            "final_row_count": final_count,
            "status": "completed"
        }
        
    except Exception as e:
        logger.error(f"Failed to merge incremental data: {str(e)}")
        return {
            "target_path": target_path,
            "status": "failed",
            "error": str(e)
        }


def process_incremental_bronze(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    watermark_column: str,
    config: Dict
) -> Dict:
    """
    Process incremental data for Bronze layer.
    
    Args:
        spark: SparkSession instance
        source_df: Source DataFrame
        target_path: Path to Bronze Delta table
        watermark_column: Watermark column name
        config: Configuration dictionary
    
    Returns:
        Dictionary with processing results
    """
    incremental_config = config.get("incremental", {})
    
    # Get current watermark
    watermark_value = get_watermark(spark, target_path, watermark_column)
    
    # Filter incremental data
    incremental_df = filter_incremental_data(
        source_df,
        watermark_column,
        watermark_value,
        incremental_config.get("initial_load_date")
    )
    
    incremental_count = incremental_df.count()
    logger.info(f"Found {incremental_count} new records to process")
    
    if incremental_count == 0:
        logger.info("No new data to process")
        return {
            "status": "skipped",
            "reason": "no_new_data",
            "watermark": watermark_value.isoformat() if watermark_value else None
        }
    
    # Write incremental data (append mode)
    partition_config = config.get("partitioning", {})
    partition_col = None
    if partition_config.get("enabled", False):
        partition_col = partition_config.get("bronze_partition_column", "trip_date")
        if partition_col not in incremental_df.columns:
            partition_col = None
    
    from etl.bronze_job import write_bronze_delta
    write_bronze_delta(incremental_df, target_path, mode="append", partition_by=partition_col)
    
    return {
        "status": "completed",
        "records_processed": incremental_count,
        "watermark": watermark_value.isoformat() if watermark_value else None
    }

