"""Metrics collection utilities for pipeline monitoring."""

from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Optional
from datetime import datetime
import logging
import json

logger = logging.getLogger("nyc_taxi_etl")


class PipelineMetrics:
    """Collects and stores pipeline execution metrics."""
    
    def __init__(self, spark: SparkSession, metrics_path: Optional[str] = None):
        """
        Initialize metrics collector.
        
        Args:
            spark: SparkSession instance
            metrics_path: Optional path to store metrics Delta table
        """
        self.spark = spark
        self.metrics_path = metrics_path
        self.metrics: Dict = {}
        self.start_time: Optional[datetime] = None
    
    def start_run(self, run_id: str, job_name: str) -> None:
        """
        Start tracking a pipeline run.
        
        Args:
            run_id: Unique run identifier
            job_name: Name of the job
        """
        self.start_time = datetime.now()
        self.metrics = {
            "run_id": run_id,
            "job_name": job_name,
            "start_time": self.start_time.isoformat(),
            "layers": {}
        }
        logger.info(f"Started metrics collection for run: {run_id}")
    
    def record_layer_metrics(
        self,
        layer: str,
        row_count_before: Optional[int] = None,
        row_count_after: Optional[int] = None,
        duration_seconds: Optional[float] = None,
        additional_metrics: Optional[Dict] = None
    ) -> None:
        """
        Record metrics for a layer.
        
        Args:
            layer: Layer name (bronze, silver, gold)
            row_count_before: Row count before processing
            row_count_after: Row count after processing
            duration_seconds: Processing duration
            additional_metrics: Additional metrics dictionary
        """
        layer_metrics = {
            "row_count_before": row_count_before,
            "row_count_after": row_count_after,
            "duration_seconds": duration_seconds,
            "rows_filtered": None,
            "rows_deduplicated": None
        }
        
        if row_count_before is not None and row_count_after is not None:
            layer_metrics["rows_filtered"] = row_count_before - row_count_after
        
        if additional_metrics:
            layer_metrics.update(additional_metrics)
        
        self.metrics["layers"][layer] = layer_metrics
        logger.info(f"Recorded metrics for layer {layer}: {layer_metrics}")
    
    def get_summary(self) -> Dict:
        """
        Get metrics summary.
        
        Returns:
            Dictionary with metrics summary
        """
        end_time = datetime.now()
        total_duration = (end_time - self.start_time).total_seconds() if self.start_time else 0.0
        
        summary = {
            **self.metrics,
            "end_time": end_time.isoformat(),
            "total_duration_seconds": total_duration,
            "layers_processed": len(self.metrics.get("layers", {}))
        }
        
        return summary
    
    def save_metrics(self) -> None:
        """Save metrics to Delta table if path is configured."""
        if not self.metrics_path:
            logger.warning("Metrics path not configured, skipping save")
            return
        
        try:
            from delta.tables import DeltaTable
            
            # Try to load existing table
            try:
                delta_table = DeltaTable.forPath(self.spark, self.metrics_path)
                logger.info(f"Metrics table exists at: {self.metrics_path}")
            except Exception:
                # Create table
                logger.info(f"Creating metrics table at: {self.metrics_path}")
                schema = """
                    run_id STRING,
                    job_name STRING,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    total_duration_seconds DOUBLE,
                    layers_processed INT,
                    metrics_json STRING,
                    created_at TIMESTAMP
                """
                empty_df = self.spark.createDataFrame([], schema=schema)
                empty_df.write.format("delta").mode("overwrite").save(self.metrics_path)
            
            # Prepare metrics row
            summary = self.get_summary()
            row = {
                "run_id": summary["run_id"],
                "job_name": summary["job_name"],
                "start_time": datetime.fromisoformat(summary["start_time"]),
                "end_time": datetime.fromisoformat(summary["end_time"]),
                "total_duration_seconds": summary["total_duration_seconds"],
                "layers_processed": summary["layers_processed"],
                "metrics_json": json.dumps(summary),
                "created_at": datetime.now()
            }
            
            # Append metrics
            metrics_df = self.spark.createDataFrame([row])
            (
                metrics_df.write
                .format("delta")
                .mode("append")
                .save(self.metrics_path)
            )
            
            logger.info("Metrics saved successfully")
            
        except Exception as e:
            logger.warning(f"Failed to save metrics: {str(e)}")


def collect_performance_metrics(func):
    """
    Decorator to collect performance metrics for a function.
    
    Args:
        func: Function to decorate
    
    Returns:
        Decorated function
    """
    def wrapper(*args, **kwargs):
        from datetime import datetime
        start_time = datetime.now()
        
        try:
            result = func(*args, **kwargs)
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"Function {func.__name__} executed in {duration:.2f} seconds")
            
            return result
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.error(f"Function {func.__name__} failed after {duration:.2f} seconds: {str(e)}")
            raise
    
    return wrapper

