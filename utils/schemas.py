"""Schema definitions for Bronze, Silver, and Gold layers."""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType, LongType
)


def get_bronze_schema() -> StructType:
    """
    Define explicit schema for Bronze layer (raw data).
    
    This schema matches the expected NYC Yellow Taxi CSV structure.
    Additional metadata columns (ingestion_ts, source_file) are added during ingestion.
    
    Returns:
        StructType schema for Bronze layer
    """
    return StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", StringType(), True),  # Will be cast to timestamp
        StructField("tpep_dropoff_datetime", StringType(), True),  # Will be cast to timestamp
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
        # Metadata columns added during ingestion
        StructField("ingestion_ts", TimestampType(), True),
        StructField("source_file", StringType(), True),
    ])


def get_silver_schema() -> StructType:
    """
    Define explicit schema for Silver layer (cleaned and conformed data).
    
    All columns are standardized to lowercase with proper types.
    
    Returns:
        StructType schema for Silver layer
    """
    return StructType([
        StructField("vendorid", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("ratecodeid", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("pulocationid", IntegerType(), True),
        StructField("dolocationid", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
        # Metadata columns
        StructField("ingestion_ts", TimestampType(), True),
        StructField("source_file", StringType(), True),
    ])


def get_gold_daily_kpis_schema() -> StructType:
    """
    Define schema for Gold Daily KPIs table.
    
    Returns:
        StructType schema for Daily KPIs table
    """
    return StructType([
        StructField("trip_date", DateType(), False),
        StructField("daily_trip_count", LongType(), False),
        StructField("daily_total_revenue", DoubleType(), False),
        StructField("avg_trip_distance", DoubleType(), True),
        StructField("avg_passenger_count", DoubleType(), True),
    ])


def get_gold_zone_demand_schema() -> StructType:
    """
    Define schema for Gold Zone Demand table.
    
    Returns:
        StructType schema for Zone Demand table
    """
    return StructType([
        StructField("trip_date", DateType(), False),
        StructField("pu_location_id", IntegerType(), True),
        StructField("trip_count", LongType(), False),
        StructField("total_revenue", DoubleType(), False),
    ])


def validate_schema(df_schema: StructType, expected_schema: StructType, allow_extra_columns: bool = True) -> tuple[bool, list[str]]:
    """
    Validate that a DataFrame schema matches the expected schema.
    
    Args:
        df_schema: Actual DataFrame schema
        expected_schema: Expected schema definition
        allow_extra_columns: If True, allow additional columns not in expected schema
    
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    expected_fields = {field.name.lower(): field for field in expected_schema.fields}
    actual_fields = {field.name.lower(): field for field in df_schema.fields}
    
    # Check for missing required fields
    for field_name, expected_field in expected_fields.items():
        if field_name not in actual_fields:
            errors.append(f"Missing required field: {expected_field.name}")
        else:
            actual_field = actual_fields[field_name]
            # Check type compatibility (allowing some flexibility)
            if not _are_types_compatible(actual_field.dataType, expected_field.dataType):
                errors.append(
                    f"Type mismatch for field '{expected_field.name}': "
                    f"expected {expected_field.dataType}, got {actual_field.dataType}"
                )
            # Check nullability (warn if expected non-null but got nullable)
            if not expected_field.nullable and actual_field.nullable:
                errors.append(
                    f"Nullability mismatch for field '{expected_field.name}': "
                    f"expected non-nullable, but field is nullable"
                )
    
    # Check for unexpected extra columns
    if not allow_extra_columns:
        for field_name in actual_fields:
            if field_name not in expected_fields:
                errors.append(f"Unexpected field: {actual_fields[field_name].name}")
    
    return len(errors) == 0, errors


def _are_types_compatible(actual_type, expected_type) -> bool:
    """
    Check if two Spark data types are compatible.
    
    Args:
        actual_type: Actual data type
        expected_type: Expected data type
    
    Returns:
        True if types are compatible
    """
    # Exact match
    if str(actual_type) == str(expected_type):
        return True
    
    # String can be cast to timestamp (common in Bronze layer)
    if isinstance(expected_type, TimestampType) and isinstance(actual_type, StringType):
        return True
    
    # Integer can be cast to Long
    if isinstance(expected_type, LongType) and isinstance(actual_type, IntegerType):
        return True
    
    # Double can be cast from Integer
    if isinstance(expected_type, DoubleType) and isinstance(actual_type, IntegerType):
        return True
    
    return False


def enforce_schema(df, expected_schema: StructType, layer_name: str = "unknown") -> tuple:
    """
    Enforce schema on a DataFrame, casting columns to match expected types.
    
    Args:
        df: Input DataFrame
        expected_schema: Expected schema
        layer_name: Name of the layer (for logging)
    
    Returns:
        Tuple of (DataFrame with enforced schema, list of warnings)
    """
    from pyspark.sql.functions import col, to_timestamp, to_date
    from pyspark.sql.types import TimestampType, DateType, StringType
    
    warnings = []
    df_enforced = df
    
    expected_fields = {field.name.lower(): field for field in expected_schema.fields}
    
    # Rename columns to match expected schema (case-insensitive)
    for actual_col in df.columns:
        actual_col_lower = actual_col.lower()
        if actual_col_lower in expected_fields:
            expected_name = expected_fields[actual_col_lower].name
            if actual_col != expected_name:
                df_enforced = df_enforced.withColumnRenamed(actual_col, expected_name)
    
    # Cast columns to match expected types
    for field in expected_schema.fields:
        field_name = field.name
        expected_type = field.dataType
        
        if field_name not in df_enforced.columns:
            # Column doesn't exist - skip (will be handled by schema validation)
            continue
        
        actual_type = dict(df_enforced.dtypes)[field_name]
        
        # Cast if needed
        if str(actual_type) != str(expected_type):
            try:
                if isinstance(expected_type, TimestampType) and isinstance(actual_type, StringType):
                    df_enforced = df_enforced.withColumn(field_name, to_timestamp(col(field_name)))
                elif isinstance(expected_type, DateType) and isinstance(actual_type, StringType):
                    df_enforced = df_enforced.withColumn(field_name, to_date(col(field_name)))
                else:
                    df_enforced = df_enforced.withColumn(field_name, col(field_name).cast(expected_type))
                warnings.append(f"Casted {field_name} from {actual_type} to {expected_type}")
            except Exception as e:
                warnings.append(f"Failed to cast {field_name}: {str(e)}")
    
    return df_enforced, warnings

