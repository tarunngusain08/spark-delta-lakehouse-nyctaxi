"""Task definitions for the ETL pipeline DAG."""

from typing import Dict
from pyspark.sql import SparkSession
from orchestration.dag import Task


def create_nyc_taxi_dag(spark: SparkSession, config: Dict) -> 'DAG':
    """
    Create the NYC Taxi ETL pipeline DAG.
    
    Args:
        spark: SparkSession instance
        config: Configuration dictionary
    
    Returns:
        Configured DAG instance
    """
    from orchestration.dag import DAG, Task
    from etl.bronze_job import run_bronze_job
    from etl.silver_job import run_silver_job
    from etl.gold_job import run_gold_job
    from etl.dq_metrics import generate_run_id
    
    dag = DAG(dag_id="nyc_taxi_etl", description="NYC Taxi Data Lakehouse ETL Pipeline")
    
    run_id = generate_run_id()
    
    # Bronze task
    bronze_task = Task(
        task_id="bronze",
        task_function=lambda: run_bronze_job(spark, config, run_id=run_id),
        dependencies=[],
        retries=1,
        description="Ingest raw CSV data into Bronze layer"
    )
    
    # Silver task (depends on Bronze)
    silver_task = Task(
        task_id="silver",
        task_function=lambda: run_silver_job(spark, config, run_id=run_id),
        dependencies=["bronze"],
        retries=1,
        description="Clean and deduplicate data in Silver layer"
    )
    
    # Gold task (depends on Silver)
    gold_task = Task(
        task_id="gold",
        task_function=lambda: run_gold_job(spark, config, run_id=run_id),
        dependencies=["silver"],
        retries=1,
        description="Create aggregated analytics tables in Gold layer"
    )
    
    dag.add_task(bronze_task)
    dag.add_task(silver_task)
    dag.add_task(gold_task)
    
    return dag

