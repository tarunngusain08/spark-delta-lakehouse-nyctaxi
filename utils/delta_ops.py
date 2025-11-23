"""Delta Lake operations utilities (OPTIMIZE, Z-ORDER, VACUUM, Time Travel)."""

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from typing import List, Optional, Dict
import logging

logger = logging.getLogger("nyc_taxi_etl")


def optimize_table(
    spark: SparkSession,
    table_path: str,
    z_order_columns: Optional[List[str]] = None,
    where_clause: Optional[str] = None
) -> Dict:
    """
    Optimize a Delta table by compacting small files and optionally applying Z-ORDER.
    
    Args:
        spark: SparkSession instance
        table_path: Path to Delta table
        z_order_columns: Optional list of columns for Z-ORDER optimization
        where_clause: Optional WHERE clause to optimize specific partitions
    
    Returns:
        Dictionary with optimization results
    """
    try:
        logger.info(f"Optimizing Delta table: {table_path}")
        
        delta_table = DeltaTable.forPath(spark, table_path)
        
        # Build OPTIMIZE command
        if where_clause:
            optimize_df = delta_table.optimize().where(where_clause).executeCompaction()
        else:
            optimize_df = delta_table.optimize().executeCompaction()
        
        # Apply Z-ORDER if specified
        if z_order_columns:
            logger.info(f"Applying Z-ORDER on columns: {z_order_columns}")
            zorder_df = delta_table.optimize().zOrder(z_order_columns).executeCompaction()
        
        # Get metrics
        metrics = {
            "table_path": table_path,
            "z_order_applied": z_order_columns is not None,
            "z_order_columns": z_order_columns,
            "status": "completed"
        }
        
        logger.info(f"Optimization completed for: {table_path}")
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to optimize table {table_path}: {str(e)}")
        return {
            "table_path": table_path,
            "status": "failed",
            "error": str(e)
        }


def vacuum_table(
    spark: SparkSession,
    table_path: str,
    retention_hours: int = 168  # Default 7 days
) -> Dict:
    """
    Run VACUUM on a Delta table to remove old files.
    
    Args:
        spark: SparkSession instance
        table_path: Path to Delta table
        retention_hours: Retention period in hours (default: 168 = 7 days)
    
    Returns:
        Dictionary with VACUUM results
    """
    try:
        logger.info(f"Running VACUUM on Delta table: {table_path} (retention: {retention_hours} hours)")
        
        delta_table = DeltaTable.forPath(spark, table_path)
        
        # Run VACUUM
        delta_table.vacuum(retentionHours=retention_hours)
        
        metrics = {
            "table_path": table_path,
            "retention_hours": retention_hours,
            "status": "completed"
        }
        
        logger.info(f"VACUUM completed for: {table_path}")
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to run VACUUM on table {table_path}: {str(e)}")
        return {
            "table_path": table_path,
            "status": "failed",
            "error": str(e)
        }


def get_table_history(
    spark: SparkSession,
    table_path: str,
    limit: int = 20
) -> 'pyspark.sql.DataFrame':
    """
    Get version history for a Delta table (time travel).
    
    Args:
        spark: SparkSession instance
        table_path: Path to Delta table
        limit: Maximum number of versions to return
    
    Returns:
        DataFrame with table history
    """
    try:
        delta_table = DeltaTable.forPath(spark, table_path)
        history_df = delta_table.history(limit)
        return history_df
    except Exception as e:
        logger.error(f"Failed to get table history for {table_path}: {str(e)}")
        return spark.createDataFrame([], schema="version LONG")


def read_table_version(
    spark: SparkSession,
    table_path: str,
    version: Optional[int] = None,
    timestamp: Optional[str] = None
) -> 'pyspark.sql.DataFrame':
    """
    Read a specific version or timestamp of a Delta table (time travel).
    
    Args:
        spark: SparkSession instance
        table_path: Path to Delta table
        version: Optional version number to read
        timestamp: Optional timestamp to read (format: "YYYY-MM-DD HH:MM:SS")
    
    Returns:
        DataFrame at specified version/timestamp
    """
    try:
        if version is not None:
            logger.info(f"Reading Delta table {table_path} at version {version}")
            df = spark.read.format("delta").option("versionAsOf", version).load(table_path)
        elif timestamp:
            logger.info(f"Reading Delta table {table_path} at timestamp {timestamp}")
            df = spark.read.format("delta").option("timestampAsOf", timestamp).load(table_path)
        else:
            logger.warning("No version or timestamp specified, reading latest")
            df = spark.read.format("delta").load(table_path)
        
        return df
    except Exception as e:
        logger.error(f"Failed to read table version: {str(e)}")
        raise


def restore_table_version(
    spark: SparkSession,
    table_path: str,
    version: int
) -> Dict:
    """
    Restore a Delta table to a specific version.
    
    Args:
        spark: SparkSession instance
        table_path: Path to Delta table
        version: Version number to restore to
    
    Returns:
        Dictionary with restore results
    """
    try:
        logger.info(f"Restoring Delta table {table_path} to version {version}")
        
        delta_table = DeltaTable.forPath(spark, table_path)
        delta_table.restoreToVersion(version)
        
        metrics = {
            "table_path": table_path,
            "restored_to_version": version,
            "status": "completed"
        }
        
        logger.info(f"Table restored to version {version}")
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to restore table version: {str(e)}")
        return {
            "table_path": table_path,
            "status": "failed",
            "error": str(e)
        }


def get_table_info(spark: SparkSession, table_path: str) -> Dict:
    """
    Get information about a Delta table.
    
    Args:
        spark: SparkSession instance
        table_path: Path to Delta table
    
    Returns:
        Dictionary with table information
    """
    try:
        delta_table = DeltaTable.forPath(spark, table_path)
        detail_df = delta_table.detail()
        detail = detail_df.collect()[0] if detail_df.count() > 0 else {}
        
        # Get history for version count
        history_df = delta_table.history(1)
        latest_version = history_df.select("version").first()["version"] if history_df.count() > 0 else None
        
        return {
            "table_path": table_path,
            "format": detail.get("format", "unknown"),
            "location": detail.get("location", table_path),
            "latest_version": latest_version,
            "num_files": detail.get("numFiles", 0),
            "size_in_bytes": detail.get("sizeInBytes", 0)
        }
    except Exception as e:
        logger.error(f"Failed to get table info: {str(e)}")
        return {
            "table_path": table_path,
            "status": "error",
            "error": str(e)
        }

