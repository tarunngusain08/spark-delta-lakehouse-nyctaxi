"""Script for Delta Lake time travel operations."""

import argparse
import yaml
from pyspark.sql import SparkSession

from utils.spark import get_spark, stop_spark
from utils.logging_utils import setup_logging
from utils.delta_ops import get_table_history, read_table_version, restore_table_version


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load configuration from YAML file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def main():
    """Main entry point for time travel operations."""
    parser = argparse.ArgumentParser(description="Delta Lake Time Travel")
    parser.add_argument(
        "--config",
        type=str,
        default="config/config.yaml",
        help="Path to configuration YAML file"
    )
    parser.add_argument(
        "operation",
        type=str,
        choices=["history", "read", "restore"],
        help="Operation: history, read, or restore"
    )
    parser.add_argument(
        "--table",
        type=str,
        required=True,
        help="Table name (bronze, silver, gold_daily_kpis, gold_zone_demand)"
    )
    parser.add_argument(
        "--version",
        type=int,
        help="Version number (for read or restore)"
    )
    parser.add_argument(
        "--timestamp",
        type=str,
        help="Timestamp string (for read, format: YYYY-MM-DD HH:MM:SS)"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output path for read operation"
    )
    
    args = parser.parse_args()
    
    logger = setup_logging(log_level="INFO")
    config = load_config(args.config)
    
    table_path = config["paths"].get(args.table)
    if not table_path:
        logger.error(f"Table {args.table} not found in config")
        return
    
    spark = get_spark("Time_Travel", config.get("spark", {}).get("master", "local[*]"))
    
    try:
        if args.operation == "history":
            logger.info(f"Getting history for table: {args.table}")
            history_df = get_table_history(spark, table_path, limit=20)
            history_df.show(truncate=False)
            
        elif args.operation == "read":
            if not args.version and not args.timestamp:
                logger.error("Must specify --version or --timestamp for read operation")
                return
            
            logger.info(f"Reading table {args.table} at version {args.version or args.timestamp}")
            df = read_table_version(
                spark,
                table_path,
                version=args.version,
                timestamp=args.timestamp
            )
            
            if args.output:
                logger.info(f"Writing to {args.output}")
                df.write.format("delta").mode("overwrite").save(args.output)
            else:
                df.show(20, truncate=False)
                
        elif args.operation == "restore":
            if not args.version:
                logger.error("Must specify --version for restore operation")
                return
            
            logger.warning(f"Restoring table {args.table} to version {args.version}")
            result = restore_table_version(spark, table_path, args.version)
            logger.info(f"Restore result: {result}")
            
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()

