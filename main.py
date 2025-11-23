"""Main orchestration script for the Data Lakehouse ETL pipeline."""

import argparse
import yaml
from pathlib import Path
from pyspark.sql import SparkSession

from utils.spark import get_spark, stop_spark
from utils.logging_utils import setup_logging
from etl.bronze_job import run_bronze_job
from etl.silver_job import run_silver_job
from etl.gold_job import run_gold_job
from etl.dq_metrics import generate_run_id
from typing import Optional


def load_config(config_path: str = "config/config.yaml", env: Optional[str] = None) -> dict:
    """
    Load configuration from YAML file with environment support.
    
    Args:
        config_path: Path to base configuration YAML file
        env: Optional environment name (dev, prod). If None, uses config.yaml
    
    Returns:
        Configuration dictionary
    """
    import os
    
    # Determine config file based on environment
    if env:
        env_config_path = f"config/{env}.yaml"
        if os.path.exists(env_config_path):
            config_path = env_config_path
            logger.info(f"Loading {env} environment config from: {config_path}")
        else:
            logger.warning(f"Environment config {env_config_path} not found, using base config")
    elif os.getenv("ENVIRONMENT"):
        env = os.getenv("ENVIRONMENT")
        env_config_path = f"config/{env}.yaml"
        if os.path.exists(env_config_path):
            config_path = env_config_path
            logger.info(f"Loading {env} environment config from: {config_path}")
    
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    return config


def main():
    """Main entry point for the ETL pipeline."""
    parser = argparse.ArgumentParser(
        description="NYC Taxi Data Lakehouse ETL Pipeline"
    )
    parser.add_argument(
        "--job",
        type=str,
        choices=["bronze", "silver", "gold", "all"],
        default="all",
        help="ETL job to run: bronze, silver, gold, or all"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config/config.yaml",
        help="Path to configuration YAML file"
    )
    parser.add_argument(
        "--env",
        type=str,
        choices=["dev", "prod"],
        help="Environment name (dev, prod). Overrides --config"
    )
    parser.add_argument(
        "--use-dag",
        action="store_true",
        help="Use DAG orchestration framework instead of sequential execution"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(log_level="INFO")
    
    # Load configuration
    config = load_config(args.config, env=args.env)
    logger.info(f"Loaded configuration for environment: {config.get('environment', 'local')}")
    
    # Create Spark session
    spark_config = config.get("spark", {})
    app_name = spark_config.get("app_name", "NYC_Taxi_Lakehouse_ETL")
    master = spark_config.get("master", "local[*]")
    
    logger.info(f"Creating Spark session: {app_name} on {master}")
    spark = get_spark(app_name, master)
    
    # Generate run ID for this pipeline execution
    run_id = generate_run_id()
    logger.info(f"Pipeline run ID: {run_id}")
    
    try:
        # Use DAG orchestration if requested
        if args.use_dag and args.job == "all":
            logger.info("Using DAG orchestration framework")
            from orchestration.tasks import create_nyc_taxi_dag
            dag = create_nyc_taxi_dag(spark, config)
            dag_result = dag.execute()
            logger.info(f"DAG execution summary: {dag_result}")
            return
        
        # Run jobs based on argument
        if args.job == "bronze":
            logger.info("Running Bronze job only")
            bronze_metadata = run_bronze_job(spark, config, run_id=run_id)
            logger.info(f"Bronze job metadata: {bronze_metadata}")
            
        elif args.job == "silver":
            logger.info("Running Silver job only")
            silver_metadata = run_silver_job(spark, config, run_id=run_id)
            logger.info(f"Silver job metadata: {silver_metadata}")
            
        elif args.job == "gold":
            logger.info("Running Gold job only")
            gold_metadata = run_gold_job(spark, config, run_id=run_id)
            logger.info(f"Gold job metadata: {gold_metadata}")
            
        elif args.job == "all":
            logger.info("Running all jobs in sequence: Bronze -> Silver -> Gold")
            bronze_metadata = run_bronze_job(spark, config, run_id=run_id)
            silver_metadata = run_silver_job(spark, config, run_id=run_id)
            gold_metadata = run_gold_job(spark, config, run_id=run_id)
            
            logger.info("=" * 60)
            logger.info("Pipeline Execution Summary")
            logger.info("=" * 60)
            logger.info(f"Run ID: {run_id}")
            logger.info(f"Bronze: {bronze_metadata.get('status', 'unknown')} - {bronze_metadata.get('final_row_count', 0)} rows")
            logger.info(f"Silver: {silver_metadata.get('status', 'unknown')} - {silver_metadata.get('final_row_count', 0)} rows")
            logger.info(f"Gold: {gold_metadata.get('status', 'unknown')}")
            logger.info("=" * 60)
            
        logger.info("ETL pipeline execution completed successfully")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}", exc_info=True)
        raise
        
    finally:
        # Stop Spark session
        logger.info("Stopping Spark session")
        stop_spark(spark)


if __name__ == "__main__":
    main()

