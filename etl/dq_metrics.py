"""Data Quality metrics collection and storage."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit
from typing import Dict, List
from datetime import datetime
import logging

logger = logging.getLogger("nyc_taxi_etl")


def create_dq_metrics_table(spark: SparkSession, metrics_path: str) -> None:
    """
    Create the data quality metrics Delta table if it doesn't exist.
    
    Args:
        spark: SparkSession instance
        metrics_path: Path to store DQ metrics Delta table
    """
    from delta.tables import DeltaTable
    
    try:
        # Try to load existing table
        delta_table = DeltaTable.forPath(spark, metrics_path)
        logger.info(f"DQ metrics table already exists at: {metrics_path}")
    except Exception:
        # Table doesn't exist, create it
        logger.info(f"Creating DQ metrics table at: {metrics_path}")
        
        schema = """
            run_id STRING,
            layer STRING,
            check_name STRING,
            description STRING,
            violation_count LONG,
            expected_result STRING,
            severity STRING,
            passed BOOLEAN,
            error STRING,
            timestamp TIMESTAMP,
            run_timestamp TIMESTAMP
        """
        
        empty_df = spark.createDataFrame([], schema=schema)
        
        (
            empty_df.write
            .format("delta")
            .mode("overwrite")
            .save(metrics_path)
        )
        
        logger.info("DQ metrics table created successfully")


def save_dq_results(
    spark: SparkSession,
    dq_results: List[Dict],
    run_id: str,
    metrics_path: str
) -> None:
    """
    Save data quality check results to Delta table.
    
    Args:
        spark: SparkSession instance
        dq_results: List of DQ check result dictionaries
        run_id: Unique identifier for this pipeline run
        metrics_path: Path to DQ metrics Delta table
    """
    if not dq_results:
        logger.warning("No DQ results to save")
        return
    
    logger.info(f"Saving {len(dq_results)} DQ check results to metrics table")
    
    # Create DataFrame from results
    rows = []
    run_timestamp = datetime.now()
    
    for result in dq_results:
        row = {
            "run_id": run_id,
            "layer": result.get("layer", "unknown"),
            "check_name": result.get("check_name", ""),
            "description": result.get("description", ""),
            "violation_count": result.get("violation_count", 0),
            "expected_result": result.get("expected_result", ""),
            "severity": result.get("severity", "INFO"),
            "passed": result.get("passed", False),
            "error": result.get("error", None),
            "timestamp": result.get("timestamp", run_timestamp.isoformat()),
            "run_timestamp": run_timestamp
        }
        rows.append(row)
    
    results_df = spark.createDataFrame(rows)
    
    # Append to metrics table
    (
        results_df.write
        .format("delta")
        .mode("append")
        .save(metrics_path)
    )
    
    logger.info("DQ results saved successfully")


def get_dq_summary(spark: SparkSession, metrics_path: str, run_id: str = None) -> DataFrame:
    """
    Get data quality summary for a specific run or latest run.
    
    Args:
        spark: SparkSession instance
        metrics_path: Path to DQ metrics Delta table
        run_id: Optional run ID to filter by. If None, gets latest run.
    
    Returns:
        DataFrame with DQ summary
    """
    metrics_df = spark.read.format("delta").load(metrics_path)
    
    if run_id:
        summary_df = metrics_df.filter(metrics_df.run_id == run_id)
    else:
        # Get latest run
        latest_run = (
            metrics_df
            .select("run_id", "run_timestamp")
            .distinct()
            .orderBy("run_timestamp", ascending=False)
            .limit(1)
        )
        
        if latest_run.count() == 0:
            logger.warning("No DQ metrics found")
            return spark.createDataFrame([], schema="run_id STRING")
        
        latest_run_id = latest_run.first()["run_id"]
        summary_df = metrics_df.filter(metrics_df.run_id == latest_run_id)
    
    return summary_df


def generate_run_id() -> str:
    """
    Generate a unique run ID for pipeline execution.
    
    Returns:
        Unique run ID string
    """
    return f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

