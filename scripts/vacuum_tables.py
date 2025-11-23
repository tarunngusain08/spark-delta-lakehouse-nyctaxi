"""Script to run VACUUM on Delta tables."""

import argparse
import yaml
from pyspark.sql import SparkSession

from utils.spark import get_spark, stop_spark
from utils.logging_utils import setup_logging
from utils.delta_ops import vacuum_table


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load configuration from YAML file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def main():
    """Main entry point for table VACUUM."""
    parser = argparse.ArgumentParser(description="Run VACUUM on Delta tables")
    parser.add_argument(
        "--config",
        type=str,
        default="config/config.yaml",
        help="Path to configuration YAML file"
    )
    parser.add_argument(
        "--table",
        type=str,
        choices=["bronze", "silver", "gold_daily_kpis", "gold_zone_demand", "all"],
        default="all",
        help="Table to vacuum"
    )
    parser.add_argument(
        "--retention-hours",
        type=int,
        default=168,
        help="Retention period in hours (default: 168 = 7 days)"
    )
    
    args = parser.parse_args()
    
    logger = setup_logging(log_level="INFO")
    config = load_config(args.config)
    
    spark = get_spark("Vacuum_Tables", config.get("spark", {}).get("master", "local[*]"))
    
    tables_to_vacuum = []
    if args.table == "all":
        tables_to_vacuum = [
            ("bronze", config["paths"]["bronze"]),
            ("silver", config["paths"]["silver"]),
            ("gold_daily_kpis", config["paths"]["gold_daily_kpis"]),
            ("gold_zone_demand", config["paths"]["gold_zone_demand"]),
        ]
    else:
        table_path = config["paths"].get(args.table)
        if table_path:
            tables_to_vacuum = [(args.table, table_path)]
    
    for table_name, table_path in tables_to_vacuum:
        logger.info(f"Running VACUUM on table: {table_name} at {table_path}")
        result = vacuum_table(spark, table_path, retention_hours=args.retention_hours)
        logger.info(f"Result: {result}")
    
    stop_spark(spark)


if __name__ == "__main__":
    main()

