"""Data Quality framework for validation and metrics tracking."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull, sum as spark_sum
from typing import Dict, List, Optional, Callable
from datetime import datetime
import logging

logger = logging.getLogger("nyc_taxi_etl")


class DataQualityCheck:
    """Represents a single data quality check."""
    
    def __init__(
        self,
        name: str,
        description: str,
        check_function: Callable[[DataFrame], DataFrame],
        expected_result: Optional[str] = None,
        severity: str = "ERROR"  # ERROR, WARNING, INFO
    ):
        """
        Initialize a data quality check.
        
        Args:
            name: Name of the check
            description: Description of what the check validates
            check_function: Function that takes a DataFrame and returns a DataFrame with violations
            expected_result: Expected result description (e.g., "count == 0")
            severity: Severity level (ERROR, WARNING, INFO)
        """
        self.name = name
        self.description = description
        self.check_function = check_function
        self.expected_result = expected_result
        self.severity = severity
    
    def run(self, df: DataFrame) -> Dict:
        """
        Run the data quality check.
        
        Args:
            df: DataFrame to check
        
        Returns:
            Dictionary with check results
        """
        try:
            violations_df = self.check_function(df)
            violation_count = violations_df.count()
            
            result = {
                "check_name": self.name,
                "description": self.description,
                "violation_count": violation_count,
                "expected_result": self.expected_result,
                "severity": self.severity,
                "passed": violation_count == 0,
                "timestamp": datetime.now().isoformat()
            }
            
            if violation_count > 0:
                logger.warning(
                    f"DQ Check '{self.name}' failed: {violation_count} violations found. "
                    f"Expected: {self.expected_result}"
                )
            else:
                logger.info(f"DQ Check '{self.name}' passed")
            
            return result
            
        except Exception as e:
            logger.error(f"DQ Check '{self.name}' failed with error: {str(e)}")
            return {
                "check_name": self.name,
                "description": self.description,
                "violation_count": -1,  # Error indicator
                "error": str(e),
                "severity": self.severity,
                "passed": False,
                "timestamp": datetime.now().isoformat()
            }


class DataQualityFramework:
    """Framework for running data quality checks and tracking metrics."""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize the Data Quality Framework.
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.checks: List[DataQualityCheck] = []
    
    def add_check(self, check: DataQualityCheck) -> None:
        """
        Add a data quality check to the framework.
        
        Args:
            check: DataQualityCheck instance
        """
        self.checks.append(check)
    
    def add_row_count_check(self, min_rows: Optional[int] = None, max_rows: Optional[int] = None) -> None:
        """
        Add a row count check.
        
        Args:
            min_rows: Minimum expected row count
            max_rows: Maximum expected row count
        """
        def check_function(df: DataFrame) -> DataFrame:
            count_val = df.count()
            violations = []
            if min_rows is not None and count_val < min_rows:
                violations.append({"violation": f"Row count {count_val} < minimum {min_rows}"})
            if max_rows is not None and count_val > max_rows:
                violations.append({"violation": f"Row count {count_val} > maximum {max_rows}"})
            
            if violations:
                return self.spark.createDataFrame(violations)
            return self.spark.createDataFrame([], schema="violation string")
        
        expected = []
        if min_rows is not None:
            expected.append(f">= {min_rows}")
        if max_rows is not None:
            expected.append(f"<= {max_rows}")
        
        check = DataQualityCheck(
            name="row_count_check",
            description=f"Validate row count is within expected range",
            check_function=check_function,
            expected_result=(" and ".join(expected)) if expected else "Any",
            severity="WARNING"
        )
        self.add_check(check)
    
    def add_null_check(self, columns: List[str], max_null_pct: float = 0.0) -> None:
        """
        Add null value checks for specified columns.
        
        Args:
            columns: List of column names to check
            max_null_pct: Maximum allowed percentage of null values (0.0 to 1.0)
        """
        for col_name in columns:
            def make_check(col: str):
                def check_function(df: DataFrame) -> DataFrame:
                    if col not in df.columns:
                        return self.spark.createDataFrame(
                            [{"violation": f"Column {col} not found"}],
                            schema="violation string"
                        )
                    
                    total_count = df.count()
                    if total_count == 0:
                        return self.spark.createDataFrame([], schema="violation string")
                    
                    null_count = df.filter(col(col).isNull() | isnan(col(col))).count()
                    null_pct = null_count / total_count if total_count > 0 else 0.0
                    
                    if null_pct > max_null_pct:
                        return self.spark.createDataFrame(
                            [{"violation": f"Column {col} has {null_pct:.2%} null values (max allowed: {max_null_pct:.2%})"}],
                            schema="violation string"
                        )
                    return self.spark.createDataFrame([], schema="violation string")
                
                return check_function
            
            check = DataQualityCheck(
                name=f"null_check_{col_name}",
                description=f"Check null percentage for column {col_name}",
                check_function=make_check(col_name),
                expected_result=f"null_pct <= {max_null_pct:.2%}",
                severity="ERROR" if max_null_pct == 0.0 else "WARNING"
            )
            self.add_check(check)
    
    def add_range_check(self, column: str, min_val: Optional[float] = None, max_val: Optional[float] = None) -> None:
        """
        Add a range check for a numeric column.
        
        Args:
            column: Column name to check
            min_val: Minimum allowed value
            max_val: Maximum allowed value
        """
        def check_function(df: DataFrame) -> DataFrame:
            if column not in df.columns:
                return self.spark.createDataFrame(
                    [{"violation": f"Column {column} not found"}],
                    schema="violation string"
                )
            
            conditions = []
            if min_val is not None:
                conditions.append(col(column) < min_val)
            if max_val is not None:
                conditions.append(col(column) > max_val)
            
            if not conditions:
                return self.spark.createDataFrame([], schema="violation string")
            
            from functools import reduce
            from operator import or_
            filter_condition = reduce(or_, conditions)
            
            violations_df = df.filter(filter_condition).select(
                col(column).alias("violation_value")
            ).withColumn(
                "violation",
                when(col(column) < min_val if min_val else False, f"Value < {min_val}")
                .when(col(column) > max_val if max_val else False, f"Value > {max_val}")
                .otherwise("")
            )
            
            return violations_df.select("violation")
        
        expected_parts = []
        if min_val is not None:
            expected_parts.append(f">= {min_val}")
        if max_val is not None:
            expected_parts.append(f"<= {max_val}")
        
        check = DataQualityCheck(
            name=f"range_check_{column}",
            description=f"Check {column} is within valid range",
            check_function=check_function,
            expected_result=" and ".join(expected_parts),
            severity="ERROR"
        )
        self.add_check(check)
    
    def run_all_checks(self, df: DataFrame, layer_name: str = "unknown") -> List[Dict]:
        """
        Run all registered data quality checks.
        
        Args:
            df: DataFrame to validate
            layer_name: Name of the layer being validated
        
        Returns:
            List of check results
        """
        logger.info(f"Running {len(self.checks)} data quality checks on {layer_name} layer")
        
        results = []
        for check in self.checks:
            result = check.run(df)
            result["layer"] = layer_name
            results.append(result)
        
        # Summary
        passed = sum(1 for r in results if r.get("passed", False))
        failed = len(results) - passed
        
        logger.info(
            f"DQ Checks completed for {layer_name}: {passed} passed, {failed} failed out of {len(results)} total"
        )
        
        return results
    
    def get_summary_stats(self, df: DataFrame) -> Dict:
        """
        Get summary statistics for a DataFrame.
        
        Args:
            df: DataFrame to analyze
        
        Returns:
            Dictionary with summary statistics
        """
        total_rows = df.count()
        total_cols = len(df.columns)
        
        # Count nulls per column
        null_counts = {}
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_counts[col_name] = {
                "null_count": null_count,
                "null_pct": null_count / total_rows if total_rows > 0 else 0.0
            }
        
        return {
            "total_rows": total_rows,
            "total_columns": total_cols,
            "null_counts": null_counts,
            "timestamp": datetime.now().isoformat()
        }


def create_default_dq_framework(spark: SparkSession, config: Dict) -> DataQualityFramework:
    """
    Create a default Data Quality framework with common checks.
    
    Args:
        spark: SparkSession instance
        config: Configuration dictionary
    
    Returns:
        Configured DataQualityFramework instance
    """
    dq_framework = DataQualityFramework(spark)
    
    # Add checks based on config
    dq_config = config.get("data_quality", {})
    
    # Range checks
    if "min_trip_distance" in dq_config:
        dq_framework.add_range_check(
            "trip_distance",
            min_val=dq_config["min_trip_distance"]
        )
    
    if "min_fare_amount" in dq_config:
        dq_framework.add_range_check(
            "fare_amount",
            min_val=dq_config["min_fare_amount"]
        )
    
    if "min_total_amount" in dq_config:
        dq_framework.add_range_check(
            "total_amount",
            min_val=dq_config["min_total_amount"]
        )
    
    # Null checks for critical columns
    critical_columns = dq_config.get("critical_columns", [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime"
    ])
    dq_framework.add_null_check(critical_columns, max_null_pct=0.0)
    
    return dq_framework

