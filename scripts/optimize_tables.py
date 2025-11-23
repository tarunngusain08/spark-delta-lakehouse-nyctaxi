"""Script to optimize Delta tables (OPTIMIZE and Z-ORDER)."""

import argparse
import yaml
from pyspark.sql import SparkSession

from utils.spark import get_spark, stop_spark
from utils.logging_utils import setup_logging
from utils.delta_ops import optimize_table


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load configuration from YAML file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def main():
    """Main entry point for table optimization."""
    parser = argparse.ArgumentParser(description="Optimize Delta tables")
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
        help="Table to optimize"
    )
    
    args = parser.parse_args()
    
    logger = setup_logging(log_level="INFO")
    config = load_config(args.config)
    
    spark = get_spark("Optimize_Tables", config.get("spark", {}).get("master", "local[*]"))
    
    optimization_config = config.get("delta_optimization", {})
    
    tables_to_optimize = []
    if args.table == "all":
        tables_to_optimize = [
            ("bronze", config["paths"]["bronze"], optimization_config.get("bronze_z_order_columns")),
            ("silver", config["paths"]["silver"], optimization_config.get("silver_z_order_columns")),
            ("gold_daily_kpis", config["paths"]["gold_daily_kpis"], optimization_config.get("gold_daily_kpis_z_order_columns")),
            ("gold_zone_demand", config["paths"]["gold_zone_demand"], optimization_config.get("gold_zone_demand_z_order_columns")),
        ]
    else:
        table_path = config["paths"].get(args.table)
        z_order_cols = optimization_config.get(f"{args.table}_z_order_columns")
        if table_path:
            tables_to_optimize = [(args.table, table_path, z_order_cols)]
    
    for table_name, table_path, z_order_cols in tables_to_optimize:
        logger.info(f"Optimizing table: {table_name} at {table_path}")
        result = optimize_table(spark, table_path, z_order_columns=z_order_cols)
        logger.info(f"Result: {result}")
    
    stop_spark(spark)


if __name__ == "__main__":
    main()

