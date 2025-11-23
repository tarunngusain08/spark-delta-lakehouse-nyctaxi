"""Audit logging utilities for pipeline execution tracking."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit
from typing import Dict, Optional, List
from datetime import datetime
import logging
import json

logger = logging.getLogger("nyc_taxi_etl")


def create_pipeline_runs_table(spark: SparkSession, runs_path: str) -> None:
    """
    Create the pipeline runs metadata Delta table if it doesn't exist.
    
    Args:
        spark: SparkSession instance
        runs_path: Path to store pipeline runs Delta table
    """
    from delta.tables import DeltaTable
    
    try:
        # Try to load existing table
        delta_table = DeltaTable.forPath(spark, runs_path)
        logger.info(f"Pipeline runs table already exists at: {runs_path}")
    except Exception:
        # Table doesn't exist, create it
        logger.info(f"Creating pipeline runs table at: {runs_path}")
        
        schema = """
            run_id STRING,
            job_name STRING,
            layer STRING,
            status STRING,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            duration_seconds DOUBLE,
            initial_row_count LONG,
            final_row_count LONG,
            rows_filtered LONG,
            rows_deduplicated LONG,
            error_message STRING,
            config_snapshot STRING,
            metadata STRING,
            created_at TIMESTAMP
        """
        
        empty_df = spark.createDataFrame([], schema=schema)
        
        (
            empty_df.write
            .format("delta")
            .mode("overwrite")
            .save(runs_path)
        )
        
        logger.info("Pipeline runs table created successfully")


def log_pipeline_run(
    spark: SparkSession,
    run_id: str,
    job_name: str,
    layer: str,
    status: str,
    start_time: datetime,
    end_time: Optional[datetime] = None,
    initial_row_count: Optional[int] = None,
    final_row_count: Optional[int] = None,
    rows_filtered: Optional[int] = None,
    rows_deduplicated: Optional[int] = None,
    error_message: Optional[str] = None,
    config_snapshot: Optional[Dict] = None,
    metadata: Optional[Dict] = None,
    runs_path: str = None
) -> None:
    """
    Log a pipeline run to the audit table.
    
    Args:
        spark: SparkSession instance
        run_id: Unique run identifier
        job_name: Name of the job (bronze, silver, gold, all)
        layer: Layer name (bronze, silver, gold)
        status: Status (running, completed, failed)
        start_time: Job start time
        end_time: Job end time (optional)
        initial_row_count: Initial row count (optional)
        final_row_count: Final row count (optional)
        rows_filtered: Number of rows filtered (optional)
        rows_deduplicated: Number of rows deduplicated (optional)
        error_message: Error message if failed (optional)
        config_snapshot: Configuration snapshot (optional)
        metadata: Additional metadata dictionary (optional)
        runs_path: Path to pipeline runs Delta table
    """
    if not runs_path:
        logger.warning("Pipeline runs path not provided, skipping audit logging")
        return
    
    try:
        end_time = end_time or datetime.now()
        duration_seconds = (end_time - start_time).total_seconds()
        
        # Prepare row data
        row_data = {
            "run_id": run_id,
            "job_name": job_name,
            "layer": layer,
            "status": status,
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": duration_seconds,
            "initial_row_count": initial_row_count,
            "final_row_count": final_row_count,
            "rows_filtered": rows_filtered,
            "rows_deduplicated": rows_deduplicated,
            "error_message": error_message,
            "config_snapshot": json.dumps(config_snapshot) if config_snapshot else None,
            "metadata": json.dumps(metadata) if metadata else None,
            "created_at": datetime.now()
        }
        
        # Create DataFrame and append
        runs_df = spark.createDataFrame([row_data])
        
        (
            runs_df.write
            .format("delta")
            .mode("append")
            .save(runs_path)
        )
        
        logger.info(f"Pipeline run logged: {run_id} - {layer} - {status}")
        
    except Exception as e:
        logger.warning(f"Failed to log pipeline run: {str(e)}")


def get_pipeline_runs_summary(
    spark: SparkSession,
    runs_path: str,
    run_id: Optional[str] = None,
    layer: Optional[str] = None,
    limit: int = 100
) -> DataFrame:
    """
    Get pipeline runs summary.
    
    Args:
        spark: SparkSession instance
        runs_path: Path to pipeline runs Delta table
        run_id: Optional run ID to filter by
        layer: Optional layer to filter by
        limit: Maximum number of records to return
    
    Returns:
        DataFrame with pipeline runs summary
    """
    try:
        runs_df = spark.read.format("delta").load(runs_path)
        
        if run_id:
            runs_df = runs_df.filter(runs_df.run_id == run_id)
        
        if layer:
            runs_df = runs_df.filter(runs_df.layer == layer)
        
        return runs_df.orderBy("start_time", ascending=False).limit(limit)
        
    except Exception as e:
        logger.error(f"Failed to get pipeline runs summary: {str(e)}")
        return spark.createDataFrame([], schema="run_id STRING")


def get_data_lineage(
    spark: SparkSession,
    runs_path: str,
    run_id: str
) -> Dict:
    """
    Get data lineage for a specific run.
    
    Args:
        spark: SparkSession instance
        runs_path: Path to pipeline runs Delta table
        run_id: Run ID to get lineage for
    
    Returns:
        Dictionary with lineage information
    """
    runs_df = spark.read.format("delta").load(runs_path)
    run_records = runs_df.filter(runs_df.run_id == run_id).orderBy("start_time").collect()
    
    lineage = {
        "run_id": run_id,
        "layers": [],
        "total_duration_seconds": 0.0,
        "status": "unknown"
    }
    
    total_duration = 0.0
    for record in run_records:
        layer_info = {
            "layer": record.layer,
            "status": record.status,
            "duration_seconds": record.duration_seconds or 0.0,
            "initial_row_count": record.initial_row_count,
            "final_row_count": record.final_row_count,
            "rows_filtered": record.rows_filtered,
            "rows_deduplicated": record.rows_deduplicated
        }
        lineage["layers"].append(layer_info)
        total_duration += record.duration_seconds or 0.0
        
        if record.status == "failed":
            lineage["status"] = "failed"
            lineage["error"] = record.error_message
    
    if lineage["status"] == "unknown":
        lineage["status"] = "completed"
    
    lineage["total_duration_seconds"] = total_duration
    
    return lineage

