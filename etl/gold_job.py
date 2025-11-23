"""Gold layer ETL job - Aggregated analytics tables."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_date, sum, count, avg, round
)
from typing import Dict, Optional
import logging

from utils.schemas import (
    get_gold_daily_kpis_schema, get_gold_zone_demand_schema,
    validate_schema, enforce_schema
)
from utils.data_quality import DataQualityFramework
from etl.dq_metrics import create_dq_metrics_table, save_dq_results, generate_run_id
from utils.delta_ops import optimize_table

logger = logging.getLogger("nyc_taxi_etl")


def read_silver_delta(spark: SparkSession, silver_path: str) -> DataFrame:
    """
    Read Silver Delta table.
    
    Args:
        spark: SparkSession instance
        silver_path: Path to Silver Delta table
    
    Returns:
        DataFrame from Silver Delta table
    """
    logger.info(f"Reading Silver Delta table from: {silver_path}")
    
    df = spark.read.format("delta").load(silver_path)
    logger.info(f"Silver table row count: {df.count()}")
    
    return df


def create_daily_kpis_table(df: DataFrame) -> DataFrame:
    """
    Create daily KPIs aggregation table.
    
    Aggregates:
    - daily_trip_count
    - daily_total_revenue (sum of total_amount)
    - avg_trip_distance
    - avg_passenger_count
    
    Args:
        df: Input DataFrame from Silver layer
    
    Returns:
        Aggregated DataFrame with daily KPIs
    """
    logger.info("Creating Daily KPIs table")
    
    # Ensure we have the required columns (case-insensitive)
    required_cols = {
        "tpep_pickup_datetime": None,
        "total_amount": None,
        "trip_distance": None,
        "passenger_count": None
    }
    
    # Find actual column names
    for req_col in list(required_cols.keys()):
        matching = [c for c in df.columns if c.lower() == req_col.lower()]
        if matching:
            required_cols[req_col] = matching[0]
        else:
            logger.warning(f"Column {req_col} not found in DataFrame")
    
    # Build aggregation
    pickup_col = required_cols.get("tpep_pickup_datetime")
    if not pickup_col:
        raise ValueError("tpep_pickup_datetime column not found")
    
    daily_kpis = (
        df
        .withColumn("trip_date", to_date(col(pickup_col)))
        .groupBy("trip_date")
        .agg(
            count("*").alias("daily_trip_count"),
            sum(required_cols.get("total_amount") or col("total_amount")).alias("daily_total_revenue"),
            avg(required_cols.get("trip_distance") or col("trip_distance")).alias("avg_trip_distance"),
            avg(required_cols.get("passenger_count") or col("passenger_count")).alias("avg_passenger_count")
        )
        .withColumn("daily_total_revenue", round(col("daily_total_revenue"), 2))
        .withColumn("avg_trip_distance", round(col("avg_trip_distance"), 2))
        .withColumn("avg_passenger_count", round(col("avg_passenger_count"), 2))
        .orderBy("trip_date")
    )
    
    logger.info(f"Daily KPIs table created. Row count: {daily_kpis.count()}")
    
    return daily_kpis


def create_zone_demand_table(df: DataFrame) -> DataFrame:
    """
    Create zone-level demand aggregation table.
    
    Aggregates by PULocationID and trip date:
    - trip_count
    - total_revenue
    
    Args:
        df: Input DataFrame from Silver layer
    
    Returns:
        Aggregated DataFrame with zone demand metrics
    """
    logger.info("Creating Zone Demand table")
    
    # Find actual column names (case-insensitive)
    pickup_col = None
    pulocation_col = None
    total_amount_col = None
    
    for col_name in df.columns:
        col_lower = col_name.lower()
        if col_lower == "tpep_pickup_datetime":
            pickup_col = col_name
        elif col_lower == "pulocationid":
            pulocation_col = col_name
        elif col_lower == "total_amount":
            total_amount_col = col_name
    
    if not pickup_col:
        raise ValueError("tpep_pickup_datetime column not found")
    if not pulocation_col:
        raise ValueError("PULocationID column not found")
    if not total_amount_col:
        raise ValueError("total_amount column not found")
    
    zone_demand = (
        df
        .withColumn("trip_date", to_date(col(pickup_col)))
        .groupBy("trip_date", pulocation_col)
        .agg(
            count("*").alias("trip_count"),
            sum(col(total_amount_col)).alias("total_revenue")
        )
        .withColumnRenamed(pulocation_col, "pu_location_id")
        .withColumn("total_revenue", round(col("total_revenue"), 2))
        .orderBy("trip_date", "pu_location_id")
    )
    
    logger.info(f"Zone Demand table created. Row count: {zone_demand.count()}")
    
    return zone_demand


def write_gold_delta(
    df: DataFrame,
    gold_path: str,
    table_name: str,
    mode: str = "overwrite",
    partition_by: Optional[str] = None
) -> None:
    """
    Write DataFrame to Gold Delta table.
    
    Args:
        df: DataFrame to write
        gold_path: Path to Gold Delta table
        table_name: Name of the table (for logging)
        mode: Write mode ("overwrite" or "append")
        partition_by: Optional column name to partition by
    """
    logger.info(f"Writing Gold Delta table '{table_name}' to: {gold_path} (mode: {mode})")
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
    
    writer.save(gold_path)
    
    logger.info(f"Gold Delta table '{table_name}' written successfully")


def run_gold_job(spark: SparkSession, config: Dict, run_id: Optional[str] = None) -> Dict:
    """
    Execute Gold layer ETL job.
    
    This job:
    1. Reads from Silver Delta table
    2. Creates Daily KPIs aggregation table
    3. Creates Zone Demand aggregation table
    4. Validates schemas
    5. Writes both to Gold Delta tables
    
    Args:
        spark: SparkSession instance
        config: Configuration dictionary with paths and settings
        run_id: Optional run ID for tracking. If None, generates one.
    
    Returns:
        Dictionary with job execution metadata
    """
    logger.info("=" * 60)
    logger.info("Starting Gold Layer ETL Job")
    logger.info("=" * 60)
    
    if run_id is None:
        run_id = generate_run_id()
    
    job_metadata = {
        "run_id": run_id,
        "layer": "gold",
        "status": "running"
    }
    
    try:
        # Read Silver Delta table
        silver_df = read_silver_delta(spark, config["paths"]["silver"])
        silver_row_count = silver_df.count()
        job_metadata["silver_row_count"] = silver_row_count
        
        # Create Daily KPIs table
        daily_kpis_df = create_daily_kpis_table(silver_df)
        
        # Schema validation for Daily KPIs
        dq_config = config.get("data_quality", {})
        if dq_config.get("enable_schema_validation", True):
            logger.info("Validating Daily KPIs schema")
            kpis_schema = get_gold_daily_kpis_schema()
            daily_kpis_df, warnings = enforce_schema(daily_kpis_df, kpis_schema, "gold_daily_kpis")
            for warning in warnings:
                logger.warning(f"Schema enforcement warning: {warning}")
            
            is_valid, errors = validate_schema(daily_kpis_df.schema, kpis_schema, allow_extra_columns=False)
            if errors:
                logger.warning(f"Daily KPIs schema validation issues: {errors[:5]}")
        
        # Get partition column if partitioning enabled
        partition_config = config.get("partitioning", {})
        partition_col = None
        if partition_config.get("enabled", False):
            partition_col = partition_config.get("gold_daily_kpis_partition_column", "trip_date")
            if partition_col not in daily_kpis_df.columns:
                partition_col = None
        
        # Determine write mode
        incremental_config = config.get("incremental", {})
        write_mode = "append" if incremental_config.get("enabled", False) else "overwrite"
        
        write_gold_delta(
            daily_kpis_df,
            config["paths"]["gold_daily_kpis"],
            "daily_kpis",
            mode=write_mode,
            partition_by=partition_col
        )
        
        # Optimize table if enabled
        optimization_config = config.get("delta_optimization", {})
        if optimization_config.get("optimize_after_write", False):
            z_order_cols = optimization_config.get("gold_daily_kpis_z_order_columns")
            optimize_table(spark, config["paths"]["gold_daily_kpis"], z_order_columns=z_order_cols)
        kpis_row_count = daily_kpis_df.count()
        job_metadata["daily_kpis_row_count"] = kpis_row_count
        
        # Create Zone Demand table
        zone_demand_df = create_zone_demand_table(silver_df)
        
        # Schema validation for Zone Demand
        if dq_config.get("enable_schema_validation", True):
            logger.info("Validating Zone Demand schema")
            zone_schema = get_gold_zone_demand_schema()
            zone_demand_df, warnings = enforce_schema(zone_demand_df, zone_schema, "gold_zone_demand")
            for warning in warnings:
                logger.warning(f"Schema enforcement warning: {warning}")
            
            is_valid, errors = validate_schema(zone_demand_df.schema, zone_schema, allow_extra_columns=False)
            if errors:
                logger.warning(f"Zone Demand schema validation issues: {errors[:5]}")
        
        # Get partition column if partitioning enabled
        partition_col = None
        if partition_config.get("enabled", False):
            partition_col = partition_config.get("gold_zone_demand_partition_column", "trip_date")
            if partition_col not in zone_demand_df.columns:
                partition_col = None
        
        write_gold_delta(
            zone_demand_df,
            config["paths"]["gold_zone_demand"],
            "zone_demand",
            mode=write_mode,
            partition_by=partition_col
        )
        
        # Optimize table if enabled
        if optimization_config.get("optimize_after_write", False):
            z_order_cols = optimization_config.get("gold_zone_demand_z_order_columns")
            optimize_table(spark, config["paths"]["gold_zone_demand"], z_order_columns=z_order_cols)
        zone_row_count = zone_demand_df.count()
        job_metadata["zone_demand_row_count"] = zone_row_count
        
        # Verify by reading back
        daily_kpis_readback = spark.read.format("delta").load(config["paths"]["gold_daily_kpis"])
        zone_demand_readback = spark.read.format("delta").load(config["paths"]["gold_zone_demand"])
        
        job_metadata["status"] = "completed"
        
        logger.info(f"Gold job completed successfully.")
        logger.info(f"  - Daily KPIs row count: {daily_kpis_readback.count()}")
        logger.info(f"  - Zone Demand row count: {zone_demand_readback.count()}")
        
        logger.info("=" * 60)
        logger.info("Gold Layer ETL Job Completed")
        logger.info("=" * 60)
        
        return job_metadata
        
    except Exception as e:
        logger.error(f"Gold job failed with error: {str(e)}", exc_info=True)
        job_metadata["status"] = "failed"
        job_metadata["error"] = str(e)
        raise

