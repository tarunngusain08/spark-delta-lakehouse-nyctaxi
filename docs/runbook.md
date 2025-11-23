# Operational Runbook

## Overview

This runbook provides operational procedures for running, monitoring, and troubleshooting the NYC Taxi Data Lakehouse ETL pipeline.

---

## Table of Contents

1. [Daily Operations](#daily-operations)
2. [Pipeline Execution](#pipeline-execution)
3. [Monitoring & Health Checks](#monitoring--health-checks)
4. [Troubleshooting](#troubleshooting)
5. [Maintenance Tasks](#maintenance-tasks)
6. [Emergency Procedures](#emergency-procedures)

---

## Daily Operations

### Standard Pipeline Run

**Frequency**: Daily (or as configured)

**Command**:
```bash
# Development
python main.py --job all --env dev

# Production
python main.py --job all --env prod
```

**Expected Duration**: 
- Bronze: 5-15 minutes (depends on data volume)
- Silver: 10-30 minutes
- Gold: 5-10 minutes
- Total: ~20-55 minutes

**Success Criteria**:
- All jobs complete with status "completed"
- No DQ errors (if `fail_on_dq_errors: true`)
- Row counts are within expected ranges

---

## Pipeline Execution

### Running Individual Jobs

```bash
# Bronze only
python main.py --job bronze

# Silver only (requires Bronze to exist)
python main.py --job silver

# Gold only (requires Silver to exist)
python main.py --job gold
```

### Using DAG Orchestration

```bash
# Run with DAG framework (includes retry logic)
python main.py --job all --use-dag
```

### Incremental Processing

**Setup**:
1. Enable in `config/prod.yaml`:
```yaml
incremental:
  enabled: true
  watermark_column: "tpep_pickup_datetime"
```

2. Run pipeline normally - it will process only new data

**First Run**:
- Set `initial_load_date` to process historical data
- Subsequent runs will be incremental

---

## Monitoring & Health Checks

### Check Pipeline Status

**Query Pipeline Runs Table**:
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Monitor").getOrCreate()

runs_df = spark.read.format("delta").load("lakehouse/metadata/pipeline_runs")
runs_df.filter("status = 'failed'").orderBy("start_time", ascending=False).show()
```

**Latest Run Status**:
```python
latest_run = runs_df.orderBy("start_time", ascending=False).first()
print(f"Status: {latest_run.status}")
print(f"Duration: {latest_run.duration_seconds} seconds")
print(f"Rows: {latest_run.final_row_count}")
```

### Check Data Quality Metrics

```python
dq_df = spark.read.format("delta").load("lakehouse/metadata/dq_metrics")
latest_dq = dq_df.orderBy("run_timestamp", ascending=False).limit(20)
failed_checks = latest_dq.filter("passed = false")
failed_checks.show(truncate=False)
```

### Monitor Row Counts

**Expected Patterns**:
- Bronze: All raw records ingested
- Silver: ~5-10% fewer rows (filtering/deduplication)
- Gold Daily KPIs: ~1 row per day
- Gold Zone Demand: Multiple rows per day (one per zone)

**Check Row Counts**:
```python
bronze_count = spark.read.format("delta").load("lakehouse/bronze/yellow_taxi_bronze").count()
silver_count = spark.read.format("delta").load("lakehouse/silver/yellow_taxi_silver").count()
gold_kpis_count = spark.read.format("delta").load("lakehouse/gold/daily_kpis").count()

print(f"Bronze: {bronze_count}")
print(f"Silver: {silver_count}")
print(f"Gold KPIs: {gold_kpis_count}")
```

---

## Troubleshooting

### Common Issues

#### 1. Pipeline Fails with Schema Validation Error

**Symptoms**: Error message about schema mismatch

**Resolution**:
1. Check source data schema:
```python
df = spark.read.option("header", "true").csv("data/raw")
df.printSchema()
```

2. Update schema in `utils/schemas.py` if needed

3. Or disable schema validation temporarily:
```yaml
data_quality:
  enable_schema_validation: false
```

#### 2. DQ Checks Failing

**Symptoms**: Pipeline fails with DQ errors

**Resolution**:
1. Check DQ metrics:
```python
dq_df = spark.read.format("delta").load("lakehouse/metadata/dq_metrics")
dq_df.filter("passed = false").show(truncate=False)
```

2. Review data quality issues
3. Adjust DQ thresholds in `config/config.yaml`
4. Or set `fail_on_dq_errors: false` for warnings only

#### 3. Out of Memory Errors

**Symptoms**: Spark executor out of memory

**Resolution**:
1. Increase Spark memory in `utils/spark.py`:
```python
.config("spark.driver.memory", "4g")
.config("spark.executor.memory", "4g")
```

2. Enable dynamic allocation:
```python
.config("spark.dynamicAllocation.enabled", "true")
```

#### 4. Small File Problem

**Symptoms**: Many small files in Delta table, slow queries

**Resolution**:
```bash
# Run OPTIMIZE
python scripts/optimize_tables.py --table all
```

#### 5. Storage Growing Too Fast

**Symptoms**: Storage costs increasing

**Resolution**:
```bash
# Run VACUUM (removes old files)
python scripts/vacuum_tables.py --table all --retention-hours 168
```

---

## Maintenance Tasks

### Weekly Tasks

#### Optimize Tables
```bash
python scripts/optimize_tables.py --table all
```

**When**: After heavy write operations

**Duration**: 10-30 minutes depending on table size

#### Check Table Health
```python
from utils.delta_ops import get_table_info

info = get_table_info(spark, "lakehouse/bronze/yellow_taxi_bronze")
print(f"Files: {info['num_files']}")
print(f"Size: {info['size_in_bytes'] / 1024 / 1024} MB")
```

### Monthly Tasks

#### VACUUM Tables
```bash
python scripts/vacuum_tables.py --table all --retention-hours 720  # 30 days
```

**Warning**: VACUUM removes old files - ensure retention period is appropriate

#### Review DQ Trends
```python
dq_df = spark.read.format("delta").load("lakehouse/metadata/dq_metrics")
# Analyze trends over time
dq_df.groupBy("check_name", "layer").agg({
    "passed": "avg",
    "violation_count": "avg"
}).show()
```

### Quarterly Tasks

#### Review and Update Schemas
- Check for schema evolution needs
- Update schema definitions if source data changes

#### Performance Tuning
- Review query performance
- Adjust Z-ORDER columns if needed
- Review partitioning strategy

---

## Emergency Procedures

### Rollback to Previous Version

**If data corruption detected**:

1. **Identify last good version**:
```bash
python scripts/time_travel.py history --table bronze
```

2. **Restore to version**:
```bash
python scripts/time_travel.py restore --table bronze --version <version_number>
```

3. **Re-run pipeline from restored point**

### Stop Pipeline Execution

**If pipeline is running incorrectly**:

1. **Find Spark application**:
```bash
# Check Spark UI or use jps
jps | grep Spark
```

2. **Kill process** (if necessary):
```bash
kill -9 <pid>
```

3. **Clean up partial writes** (if needed):
```python
# Remove incomplete Delta table versions
# Use Delta Lake utilities or manual cleanup
```

### Data Recovery

**If data is lost or corrupted**:

1. **Check Time Travel**:
```bash
python scripts/time_travel.py history --table <table_name>
```

2. **Read previous version**:
```bash
python scripts/time_travel.py read --table <table_name> --version <version>
```

3. **Restore or re-process from backup**

---

## Performance Optimization

### Query Performance

**Slow queries?** Check:
1. Partitioning enabled: `partitioning.enabled: true`
2. Z-ORDER applied: Check `delta_optimization` config
3. Table optimized: Run `optimize_tables.py`

### Write Performance

**Slow writes?** Check:
1. Partitioning strategy
2. File sizes (too many small files)
3. Spark configuration (memory, cores)

---

## Configuration Management

### Environment-Specific Configs

- **Development**: `config/dev.yaml` - Lenient DQ, no optimization
- **Production**: `config/prod.yaml` - Strict DQ, full optimization

### Changing Configuration

1. Edit appropriate config file
2. Test in dev environment first
3. Deploy to production after validation

---

## Support & Escalation

### Log Locations

- **Application logs**: Check stdout/stderr or log file if configured
- **Spark logs**: Check Spark UI or log directory
- **Delta logs**: In `_delta_log/` directory of each table

### Debugging

**Enable debug logging**:
```python
logger = setup_logging(log_level="DEBUG")
```

**Check Spark UI**: Usually at `http://localhost:4040`

---

## Best Practices

1. **Always test in dev first**
2. **Monitor DQ metrics regularly**
3. **Run OPTIMIZE weekly**
4. **Keep retention periods appropriate**
5. **Document any schema changes**
6. **Review pipeline runs daily**
7. **Set up alerts for failures**

---

## Quick Reference

### Common Commands

```bash
# Run pipeline
python main.py --job all --env prod

# Optimize tables
python scripts/optimize_tables.py --table all

# VACUUM
python scripts/vacuum_tables.py --table all

# Time travel
python scripts/time_travel.py history --table bronze

# Run tests
pytest -m integration
```

### Key Paths

- **Bronze**: `lakehouse/bronze/yellow_taxi_bronze`
- **Silver**: `lakehouse/silver/yellow_taxi_silver`
- **Gold KPIs**: `lakehouse/gold/daily_kpis`
- **Gold Zone**: `lakehouse/gold/zone_demand`
- **DQ Metrics**: `lakehouse/metadata/dq_metrics`
- **Pipeline Runs**: `lakehouse/metadata/pipeline_runs`

---

## Version History

- **v1.0**: Initial runbook
- Updated: 2024

